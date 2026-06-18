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
)
from infrastructure.broker import TaskMessage, TOPIC_REQUEST, get_broker
from infrastructure.task_store import (
    task_create, task_get, task_request_cancel,
    idempotent_check, idempotent_set,
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


@router.get("/task/{task_id}/stream")
async def task_stream(task_id: str):
    """SSE stream that polls Redis and pushes state changes.

    Reuses the existing Server-Sent Events pattern from /query/full/stream.
    """
    import asyncio
    from sse_starlette.sse import EventSourceResponse

    async def event_generator():
        last_updated = ""
        last_node = ""
        # Poll up to 5 minutes
        for _ in range(600):
            state = task_get(task_id)
            if state is None:
                yield {"event": "error", "data": json.dumps({"error": "Task not found"})}
                return

            status = state.get("status", "?")
            updated = state.get("updated_at", "")
            node = state.get("node", "")

            # Emit on state change or new node
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

            await asyncio.sleep(0.5)

        # Timeout — task still running after 5 min
        yield {"event": "timeout", "data": json.dumps({"error": "Stream timeout (5 min)"})}

    return EventSourceResponse(event_generator())
