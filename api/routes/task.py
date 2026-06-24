"""Async task endpoints — submit jobs via Kafka and poll/stream progress.

POST   /task/submit      → create task, write to Kafka, return task_id
GET    /task/{id}/status  → poll Redis for current task state
POST   /task/{id}/cancel  → request cancellation
GET    /task/{id}/stream  → SSE stream of task progress events
"""

import hashlib
import json
import uuid

from fastapi import APIRouter, HTTPException

from api.models import (
    TaskSubmitRequest,
    TaskSubmitResponse,
    TaskStatusResponse,
    TaskCancelResponse,
    TaskFeedbackRequest,
    TaskFeedbackResponse,
)
from infrastructure.broker import TaskMessage, TOPIC_REQUEST, TOPIC_FEEDBACK, get_broker
from infrastructure.task_store import (
    task_create, task_get, task_request_cancel,
    task_get_heartbeat, scan_stale_tasks,
    feedback_transition, TASK_FEEDBACK_MAX_TURNS,
    idempotent_check, idempotent_set,
    HEARTBEAT_STALE_S,
)

router = APIRouter()


@router.post("/task/submit", response_model=TaskSubmitResponse, status_code=202)
def task_submit(req: TaskSubmitRequest):
    """Submit a question for async processing. Returns immediately with task_id."""
    broker = get_broker()

    # ── Idempotency check ──
    if req.idempotency_key:
        idem_key = hashlib.sha256(
            f"{req.idempotency_key}:{req.question[:80]}".encode()
        ).hexdigest()[:32]
        existing = idempotent_check(idem_key)
        if existing:
            return TaskSubmitResponse(task_id=existing, status="PENDING")

    task_id = uuid.uuid4().hex[:12]

    if req.idempotency_key:
        idem_key = hashlib.sha256(
            f"{req.idempotency_key}:{req.question[:80]}".encode()
        ).hexdigest()[:32]
        idempotent_set(idem_key, task_id)

    # ── Build payload ──
    payload = {
        "question": req.question,
        "db_id": req.db_id or "",
        "database_url": req.database_url or "",
        "rag_schema": req.rag_schema,
        "rag_domain": req.rag_domain,
        "multi_candidate": req.multi_candidate,
        "rag_k": req.rag_k,
        "rag_column_prune": req.rag_column_prune,
        "rag_hybrid": req.rag_hybrid,
        "rag_fk_expand": req.rag_fk_expand,
        "fewshot_enabled": req.fewshot_enabled,
    }

    # ── Create Redis state ──
    task_create(task_id, req.question, req.db_id or "", req.database_url or "")

    # ── Publish to Kafka ──
    msg = TaskMessage(task_id=task_id, event="submitted", payload=payload)
    broker.publish(TOPIC_REQUEST, msg)

    return TaskSubmitResponse(task_id=task_id, status="PENDING")


@router.get("/task/{task_id}/status", response_model=TaskStatusResponse)
def task_status(task_id: str):
    """Poll current task state from Redis."""
    state = task_get(task_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Task not found or expired")
    return TaskStatusResponse(**state)


@router.post("/task/{task_id}/cancel", response_model=TaskCancelResponse)
def task_cancel(task_id: str):
    """Request cancellation. Worker checks flag between nodes."""
    state = task_get(task_id)
    if state is None:
        return TaskCancelResponse(task_id=task_id, status="not_found")
    if state.get("status") in ("SUCCESS", "FAILED", "CANCELLED", "TIMEOUT"):
        return TaskCancelResponse(task_id=task_id, status=state["status"])
    task_request_cancel(task_id)
    return TaskCancelResponse(task_id=task_id, status="cancelled")


@router.get("/task/{task_id}/health")
def task_health(task_id: str):
    """Return heartbeat health for a task (worker liveness check)."""
    from datetime import datetime, timezone

    state = task_get(task_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Task not found or expired")

    status = state.get("status", "?")
    hb = task_get_heartbeat(task_id)

    healthy = False
    stale_s = None

    if hb:
        try:
            hb_dt = datetime.fromisoformat(hb)
            elapsed = (datetime.now(timezone.utc) - hb_dt).total_seconds()
            healthy = elapsed < HEARTBEAT_STALE_S
            stale_s = round(elapsed, 1)
        except (ValueError, TypeError):
            healthy = False

    return {
        "task_id": task_id,
        "task_status": status,
        "heartbeat": hb,
        "heartbeat_stale_s": stale_s,
        "healthy": healthy,
        "worker_alive": healthy and status == "RUNNING",
    }


@router.post("/task/scan-stale")
def trigger_stale_scan():
    """Manually trigger a stale task scan (returns list of timed-out task IDs)."""
    stale = scan_stale_tasks()
    return {"stale_count": len(stale), "stale_task_ids": stale}


@router.get("/task/{task_id}/stream")
async def task_stream(task_id: str):
    """SSE stream with real-time SQL token push + status polling.

    Tokens arrive via Redis pub/sub (Worker publishes each LLM token).
    Status updates arrive via Redis polling (500ms interval).
    """
    import asyncio
    import threading
    from sse_starlette.sse import EventSourceResponse
    from storage.redis_cache import get_redis

    async def event_generator():
        loop = asyncio.get_event_loop()
        token_queue: asyncio.Queue = asyncio.Queue()
        ps_thread = None
        ps = None

        # ── Subscribe to Redis token channel (best-effort) ──
        r = get_redis()
        if r is not None:
            try:
                ps = r.pubsub(ignore_subscribe_messages=True)
                ps.subscribe(f"task:{task_id}:tokens")

                def _listen():
                    try:
                        for msg in ps.listen():
                            if msg.get("type") == "message":
                                asyncio.run_coroutine_threadsafe(
                                    token_queue.put(msg["data"]), loop
                                )
                    except Exception:
                        pass  # Redis connection closed → exit silently

                ps_thread = threading.Thread(target=_listen, daemon=True, name=f"ps-{task_id}")
                ps_thread.start()
            except Exception:
                ps = None  # Redis pub/sub unavailable → fall back to status-only

        try:
            last_updated = ""
            last_node = ""
            deadline = asyncio.get_event_loop().time() + 300  # 5 min max

            while asyncio.get_event_loop().time() < deadline:
                # ── Wait for token or timeout ──
                try:
                    token = await asyncio.wait_for(token_queue.get(), timeout=0.5)
                    yield {
                        "event": "token",
                        "data": json.dumps({"text": token}, ensure_ascii=False),
                    }
                    continue  # token arrived, check for more before polling
                except asyncio.TimeoutError:
                    pass  # no token, fall through to status poll

                # ── Status poll ──
                state = task_get(task_id)
                if state is None:
                    yield {"event": "error", "data": json.dumps({"error": "Task not found"})}
                    return

                status = state.get("status", "?")
                updated = state.get("updated_at", "")
                node = state.get("node", "")

                # Emit on state change
                if updated != last_updated:
                    last_updated = updated
                    yield {
                        "event": "status",
                        "data": json.dumps({
                            "status": status,
                            "progress": state.get("progress", 0),
                            "node": node,
                            "sql_preview": (state.get("sql") or "")[:200],
                            "error": state.get("error"),
                        }, ensure_ascii=False, default=str),
                    }

                if node and node != last_node:
                    last_node = node
                    yield {
                        "event": "node_done",
                        "data": json.dumps({"node": node}, ensure_ascii=False),
                    }

                if status in ("SUCCESS", "FAILED", "TIMEOUT", "CANCELLED"):
                    yield {
                        "event": "complete",
                        "data": json.dumps({
                            "status": status,
                            "sql": state.get("sql", ""),
                            "exec_result": state.get("exec_result"),
                            "token_usage": state.get("token_usage", {}),
                            "node_timings": state.get("node_timings", {}),
                            "error": state.get("error"),
                        }, ensure_ascii=False, default=str),
                    }
                    return

            # Timeout — task still running after 5 min
            yield {"event": "timeout", "data": json.dumps({"error": "Stream timeout (5 min)"})}
        finally:
            if ps is not None:
                try:
                    ps.unsubscribe(f"task:{task_id}:tokens")
                    ps.close()
                except Exception:
                    pass

    return EventSourceResponse(event_generator())


# ── Human-Feedback ────────────────────────────────────────────────────────────

@router.post("/task/{task_id}/feedback", response_model=TaskFeedbackResponse, status_code=202)
def task_feedback(task_id: str, req: TaskFeedbackRequest):
    """Submit human correction guidance for a completed task.

    Triggers a feedback correction round: Worker loads the task context
    (schema, few-shot, previous results) and runs the feedback graph
    (Refiner → Generator → Guard → Voter → SemCheck).
    """
    state = task_get(task_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Task not found or expired")
    cur_status = state.get("status", "")
    if cur_status not in ("SUCCESS", "FAILED"):
        raise HTTPException(status_code=400,
                            detail=f"Feedback only allowed on SUCCESS tasks (current: {cur_status})")

    turns = state.get("conversation_turns", [])
    turn_number = len(turns) + 1
    if turn_number > TASK_FEEDBACK_MAX_TURNS:
        raise HTTPException(status_code=400,
                            detail=f"Maximum feedback turns ({TASK_FEEDBACK_MAX_TURNS}) reached")

    # Publish to feedback topic for Worker to pick up
    broker = get_broker()
    msg = TaskMessage(task_id=task_id, event="feedback", payload={
        "feedback": req.feedback,
        "turn": turn_number,
        "question": state.get("question", ""),
        "db_id": state.get("db_id", ""),
        "database_url": state.get("database_url", ""),
        "sql": state.get("sql", ""),
        "exec_result": state.get("exec_result"),
        "conversation_turns": turns,
        "token_usage": state.get("token_usage", {}),
        "node_timings": state.get("node_timings", {}),
    })
    broker.publish(TOPIC_FEEDBACK, msg)

    feedback_transition(task_id)

    return TaskFeedbackResponse(task_id=task_id, status="accepted", turn=turn_number)
