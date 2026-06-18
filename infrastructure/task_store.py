"""Redis-backed task state store.

States: PENDING → RUNNING → SUCCESS / FAILED / TIMEOUT / CANCELLED

Key layout:
  task:{task_id}              → JSON state dict (TTL varies by status)
  task:{task_id}:heartbeat    → ISO timestamp (TTL 30s, refreshed every HEARTBEAT_INTERVAL_S)
  task:{task_id}:cancel       → "1" (flag, TTL 1h)
  idempotent:{key_hash}       → task_id (TTL 5 min, dedup)

All operations are best-effort — Redis unavailable = no-op.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any

_log = logging.getLogger("nl2sql.task_store")

# Progressive task timeout per retry attempt (seconds) — matches eval system
TASK_TIMEOUTS = [120, 300, 480]  # attempt 0=120s, 1=300s, 2=480s

# Heartbeat: background thread pings every 5s; stale after 60s = worker dead
HEARTBEAT_INTERVAL_S = 5
HEARTBEAT_TTL_S = 30       # expiry for the heartbeat key itself
HEARTBEAT_STALE_S = 60      # threshold to declare worker dead

# Differentiated TTLs for task state keys (seconds)
TTL_RUNNING = 7200          # PENDING / RUNNING: 2h
TTL_TERMINAL_GOOD = 86400   # SUCCESS / FAILED / CANCELLED: 24h
TTL_TERMINAL_BAD = 3600     # TIMEOUT: 1h
TTL_IDEMPOTENT = 300        # idempotency key: 5 min
TTL_CANCEL_FLAG = 3600      # cancel flag: 1h


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_redis():
    from storage.redis_cache import get_redis
    return get_redis()


# ── Task state CRUD ──────────────────────────────────────────────────────────

def task_create(task_id: str, question: str, db_id: str = "",
                database_url: str = "") -> dict:
    """Initialise a PENDING task. Returns the state dict."""
    state = {
        "task_id": task_id,
        "status": "PENDING",
        "question": question[:200],
        "db_id": db_id,
        "database_url": database_url,
        "progress": 0,
        "node": None,
        "sql": None,
        "exec_result": None,
        "token_usage": {},
        "node_timings": {},
        "retry_count": 0,
        "error": None,
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
    }
    _set_state(task_id, state)
    return state


def task_get(task_id: str) -> dict | None:
    r = _get_redis()
    if r is None:
        return None
    raw = r.get(f"task:{task_id}")
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def task_update(task_id: str, **kwargs) -> dict | None:
    """Merge kwargs into task state, return updated dict."""
    state = task_get(task_id)
    if state is None:
        return None
    state.update(kwargs)
    state["updated_at"] = _now_iso()
    _set_state(task_id, state)
    return state


def task_transition(task_id: str, status: str, **extra) -> dict | None:
    """Set status + optional fields. Blocks illegal state transitions."""
    current = task_get(task_id)
    cur_status = current.get("status", "PENDING") if current else None
    valid = _valid_transition(current, status)
    if not valid:
        _log.warning("Illegal state transition for %s: %s → %s (blocked)", task_id, cur_status, status)
        return current
    return task_update(task_id, status=status, **extra)


def _valid_transition(current: dict | None, next_status: str) -> bool:
    if current is None:
        return next_status == "PENDING"
    cur = current.get("status", "PENDING")
    allowed = {
        "PENDING":   {"RUNNING", "CANCELLED"},
        "RUNNING":   {"SUCCESS", "FAILED", "TIMEOUT", "CANCELLED"},
        "SUCCESS":   set(),
        "FAILED":    {"PENDING"},  # retry → back to pending
        "TIMEOUT":   {"PENDING"},  # retry
        "CANCELLED": set(),
    }
    return next_status in allowed.get(cur, set())


# ── Cancel ───────────────────────────────────────────────────────────────────

def task_request_cancel(task_id: str) -> bool:
    r = _get_redis()
    if r is None:
        return False
    r.setex(f"task:{task_id}:cancel", 3600, "1")
    return True


def task_is_cancelled(task_id: str) -> bool:
    r = _get_redis()
    if r is None:
        return False
    return r.exists(f"task:{task_id}:cancel") > 0


def task_clear_cancel(task_id: str) -> None:
    r = _get_redis()
    if r is None:
        return
    r.delete(f"task:{task_id}:cancel")


# ── Idempotency ──────────────────────────────────────────────────────────────

def idempotent_check(key: str) -> str | None:
    """Return existing task_id if this key was seen before, else None."""
    r = _get_redis()
    if r is None:
        return None
    return r.get(f"idempotent:{key}")


def idempotent_set(key: str, task_id: str, ttl: int = 300) -> None:
    r = _get_redis()
    if r is None:
        return
    r.setex(f"idempotent:{key}", ttl, task_id)


# ── Heartbeat ─────────────────────────────────────────────────────────────────

def task_heartbeat(task_id: str) -> None:
    """Write a heartbeat timestamp (best-effort, called every HEARTBEAT_INTERVAL_S)."""
    r = _get_redis()
    if r is None:
        return
    r.setex(f"task:{task_id}:heartbeat", HEARTBEAT_TTL_S, _now_iso())


def task_get_heartbeat(task_id: str) -> str | None:
    """Return ISO heartbeat timestamp or None (missing/expired = stale)."""
    r = _get_redis()
    if r is None:
        return None
    val = r.get(f"task:{task_id}:heartbeat")
    return val if val else None


# ── Stale task detection ──────────────────────────────────────────────────────

def scan_stale_tasks(stale_s: int = HEARTBEAT_STALE_S) -> list[str]:
    """Return task_ids in RUNNING state whose heartbeat is older than stale_s seconds.

    Call periodically (e.g. every 10s) from FastAPI lifespan or a cron.
    When a stale task is found, it is auto-transitioned to TIMEOUT.
    """
    r = _get_redis()
    if r is None:
        return []

    stale_ids: list[str] = []
    now = datetime.now(timezone.utc)

    # SCAN for task:* keys (exclude heartbeat/cancel/idempotent subkeys)
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match="task:*", count=100)
        for key in keys:
            # Only process base state keys: task:{id} (no colon after hex part)
            key_str = key if isinstance(key, str) else key.decode("utf-8")
            # Skip sub-keys: heartbeat, cancel, idempotent
            if ":heartbeat" in key_str or ":cancel" in key_str:
                continue
            task_id = key_str.replace("task:", "", 1)
            if task_id.startswith("idempotent:"):
                continue

            state = task_get(task_id)
            if state is None:
                continue
            if state.get("status") != "RUNNING":
                continue

            hb = task_get_heartbeat(task_id)
            if hb is None:
                # No heartbeat at all → stale
                stale_ids.append(task_id)
                _log.warning("Zombie detected (no heartbeat): %s → TIMEOUT", task_id)
                task_transition(task_id, "TIMEOUT",
                                error="Worker lost (no heartbeat for {}s)".format(stale_s))
                continue

            try:
                hb_dt = datetime.fromisoformat(hb)
                elapsed = (now - hb_dt).total_seconds()
                if elapsed > stale_s:
                    stale_ids.append(task_id)
                    _log.warning("Zombie detected (last heartbeat %ds ago): %s → TIMEOUT",
                                 int(elapsed), task_id)
                    task_transition(task_id, "TIMEOUT",
                                    error="Worker lost (heartbeat stale {}s)".format(int(elapsed)))
            except (ValueError, TypeError):
                stale_ids.append(task_id)
                _log.warning("Zombie detected (unparseable heartbeat): %s → TIMEOUT", task_id)
                task_transition(task_id, "TIMEOUT",
                                error="Worker lost (heartbeat unparseable)")

        if cursor == 0:
            break

    return stale_ids


def get_task_timeout(retry_count: int = 0) -> int:
    """Progressive timeout for the given retry count."""
    idx = min(retry_count, len(TASK_TIMEOUTS) - 1)
    return TASK_TIMEOUTS[idx]


# ── Internals ────────────────────────────────────────────────────────────────

def _ttl_for_status(status: str) -> int:
    """Return Redis key TTL appropriate for the task status."""
    if status in ("PENDING", "RUNNING"):
        return TTL_RUNNING
    if status == "TIMEOUT":
        return TTL_TERMINAL_BAD
    if status in ("SUCCESS", "FAILED", "CANCELLED"):
        return TTL_TERMINAL_GOOD
    return TTL_RUNNING  # fallback


def _set_state(task_id: str, state: dict) -> None:
    r = _get_redis()
    if r is None:
        return
    status = state.get("status", "PENDING")
    ttl = _ttl_for_status(status)
    r.setex(f"task:{task_id}", ttl, json.dumps(state, ensure_ascii=False, default=str))
