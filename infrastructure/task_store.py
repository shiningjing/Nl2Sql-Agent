"""Redis-backed task state store.

States: PENDING → RUNNING → SUCCESS / FAILED / TIMEOUT / CANCELLED

Key layout:
  task:{task_id}         → JSON state dict
  task:{task_id}:cancel  → "1" (flag, TTL 1h)
  idempotent:{key_hash}  → task_id (TTL 5 min, dedup)

All operations are best-effort — Redis unavailable = no-op.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any

_log = logging.getLogger("nl2sql.task_store")


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
    """Set status + optional fields. Validates legal state transitions."""
    valid = _valid_transition(task_get(task_id), status)
    if not valid:
        _log.warning("Illegal state transition for %s → %s", task_id, status)
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


# ── Internals ────────────────────────────────────────────────────────────────

def _set_state(task_id: str, state: dict) -> None:
    r = _get_redis()
    if r is None:
        return
    r.setex(f"task:{task_id}", 7200, json.dumps(state, ensure_ascii=False, default=str))
