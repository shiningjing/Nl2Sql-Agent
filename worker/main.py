"""NL2SQL Worker — consumes Kafka task messages and runs LangGraph pipeline.

Start with:
  python -m worker.main

Graceful degradation: if Kafka is unavailable, worker exits with a clear
message — the system falls back to the existing sync /query endpoints.
"""

import json
import logging
import os
import signal
import sys
import time
import traceback

# Ensure project root is on path (for python -m worker.main)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from infrastructure.broker import (
    TOPIC_REQUEST, TOPIC_STATUS, TOPIC_RESULT, TOPIC_DLQ,
    TaskMessage, get_broker,
)
from infrastructure.task_store import (
    task_create, task_get, task_update, task_transition,
    task_is_cancelled, task_clear_cancel,
)

_log = logging.getLogger("nl2sql.worker")
MAX_RETRIES = 3
TASK_TIMEOUT_S = 120


# ── LangGraph runner ─────────────────────────────────────────────────────────

def run_graph(task_id: str, payload: dict) -> dict:
    """Execute a single LangGraph pipeline invocation.

    Returns final AgentState dict. Publishes status events at each node.
    """
    from agent.graphs.full_graph import create_full_graph

    db_id = payload.get("db_id", "")
    database_url = payload.get("database_url", "")

    initial_state = {
        "question": payload["question"],
        "db_id": db_id,
        "database_url": database_url,
        "rag_schema": payload.get("rag_schema", True),
        "rag_domain": payload.get("rag_domain", True),
        "multi_candidate": payload.get("multi_candidate", True),
        "rag_k": payload.get("rag_k", 8),
        "rag_column_prune": payload.get("rag_column_prune", False),
        "rag_hybrid": payload.get("rag_hybrid", True),
        "rag_fk_expand": payload.get("rag_fk_expand", True),
        "fewshot_enabled": payload.get("fewshot_enabled", True),
    }

    graph = create_full_graph()
    t0 = time.time()

    broker = get_broker()
    accumulated = dict(initial_state)

    # Stream mode: yield each node's output, merge into accumulated state
    for step in graph.stream(initial_state, stream_mode="updates"):
        # Check cancel flag before each node
        if task_is_cancelled(task_id):
            _log.info("Task %s cancelled mid-execution", task_id)
            task_transition(task_id, "CANCELLED")
            broker.publish(TOPIC_STATUS, TaskMessage(
                task_id=task_id, event="cancelled",
                payload={"node": step.get("_last_node", "?")},
            ))
            task_clear_cancel(task_id)
            return {"_cancelled": True}

        for node_name, node_output in step.items():
            if isinstance(node_output, dict):
                accumulated.update(node_output)

            # Publish progress
            summary = _summarize_node(node_name, accumulated)
            broker.publish(TOPIC_STATUS, TaskMessage(
                task_id=task_id, event="node_done",
                payload={"node": node_name, "summary": summary},
            ))

            # Update Redis
            task_update(task_id, node=node_name, progress=_node_progress(node_name),
                        sql=accumulated.get("sql") or accumulated.get("chosen_sql"),
                        token_usage=accumulated.get("token_usage", {}),
                        node_timings=accumulated.get("node_latency", {}))

    elapsed = round(time.time() - t0, 2)
    accumulated["_worker_elapsed"] = elapsed

    # Extract final results
    sql = accumulated.get("sql") or accumulated.get("chosen_sql", "")
    exec_result = accumulated.get("exec_result")

    task_update(task_id, sql=sql, exec_result=_sanitize(exec_result),
                token_usage=accumulated.get("token_usage", {}),
                node_timings=accumulated.get("node_latency", {}))

    broker.publish(TOPIC_RESULT, TaskMessage(
        task_id=task_id, event="success",
        payload={
            "sql": sql,
            "exec_result": _sanitize(exec_result),
            "token_usage": accumulated.get("token_usage", {}),
            "node_timings": accumulated.get("node_latency", {}),
            "elapsed_s": elapsed,
        },
    ))

    return accumulated


def _node_progress(node_name: str) -> int:
    """Map node name to approximate progress percentage."""
    order = {"router": 5, "schema_retriever": 15, "decomposer": 25,
             "fewshot_selector": 35, "generator": 55, "guard": 65,
             "voter": 85, "semantic_check": 95, "refiner": 80}
    return order.get(node_name, 50)


def _summarize_node(node_name: str, state: dict) -> dict:
    """Human-readable summary of node output for status events."""
    s = {}
    if node_name == "schema_retriever":
        s["schema_len"] = len(state.get("schema_text", ""))
    elif node_name == "router":
        s["complexity"] = state.get("complexity", "simple")
    elif node_name == "decomposer":
        s["sub_count"] = len(state.get("sub_questions", []))
    elif node_name == "fewshot_selector":
        s["hit_count"] = len(state.get("fewshot_hits", []))
    elif node_name == "generator":
        s["candidate_count"] = len(state.get("candidate_sqls", []) or [])
        s["sql_preview"] = (state.get("sql") or "")[:200]
    elif node_name == "guard":
        s["guard_pass"] = state.get("guard_pass", False)
    elif node_name == "voter":
        er = state.get("exec_result", {}) or {}
        s["exec_success"] = er.get("success", False)
        s["row_count"] = er.get("row_count", 0)
    elif node_name == "semantic_check":
        s["semantic_pass"] = state.get("semantic_pass", True)
    elif node_name == "refiner":
        s["retry_count"] = state.get("retry_count", 0)
    return s


def _sanitize(result: dict | None) -> dict | None:
    """Convert Decimal / Row objects to JSON-safe types."""
    if result is None:
        return None
    from decimal import Decimal
    out = dict(result)
    data = out.get("data")
    if data:
        def _safe(v):
            if isinstance(v, Decimal):
                return float(v)
            return v
        out["data"] = [tuple(_safe(v) for v in row) for row in data]
    return out


# ── Message handler ──────────────────────────────────────────────────────────

def handle_task(msg: TaskMessage) -> None:
    """Callback from Kafka consumer — run the task pipeline."""
    task_id = msg.task_id
    payload = msg.payload
    retry_count = payload.get("_retry_count", 0)

    # Check if cancelled before starting
    if task_is_cancelled(task_id):
        task_transition(task_id, "CANCELLED")
        _log.info("Task %s cancelled before start", task_id)
        return

    task_transition(task_id, "RUNNING")
    broker = get_broker()
    broker.publish(TOPIC_STATUS, TaskMessage(
        task_id=task_id, event="running",
    ))

    try:
        run_graph(task_id, payload)
        task_transition(task_id, "SUCCESS")
    except Exception as e:
        err_msg = f"{type(e).__name__}: {str(e)[:300]}"
        _log.error("Task %s failed (attempt %d/%d): %s",
                   task_id, retry_count + 1, MAX_RETRIES + 1, err_msg)

        if retry_count < MAX_RETRIES:
            # Re-publish to request topic with incremented retry count
            payload["_retry_count"] = retry_count + 1
            payload["_last_error"] = err_msg
            broker.publish(TOPIC_REQUEST, TaskMessage(task_id=task_id, event="retry", payload=payload))
            task_update(task_id, retry_count=retry_count + 1, error=err_msg)
            broker.publish(TOPIC_STATUS, TaskMessage(
                task_id=task_id, event="retrying",
                payload={"attempt": retry_count + 1, "error": err_msg},
            ))
        else:
            # Dead-letter — retries exhausted
            task_transition(task_id, "FAILED", error=err_msg)
            broker.publish(TOPIC_STATUS, TaskMessage(
                task_id=task_id, event="failed",
                payload={"error": err_msg, "retries_exhausted": True},
            ))
            broker.publish(TOPIC_DLQ, TaskMessage(
                task_id=task_id, event="dead_letter",
                payload={"original_payload": payload, "error": err_msg,
                         "traceback": traceback.format_exc()[-2000:]},
            ))


# ── Main loop ────────────────────────────────────────────────────────────────

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    _log.info("NL2SQL Worker starting...")

    broker = get_broker()

    # Ensure topics exist
    broker.create_topics()

    # Graceful shutdown
    running = [True]

    def _shutdown(sig, frame):
        _log.info("Received signal %s, shutting down...", sig)
        running[0] = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    _log.info("Worker listening on topics: %s", TOPIC_REQUEST)

    # Kafka consumer loop runs in foreground; signal handler sets running=False
    # which stops the iterator after the current poll cycle
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            TOPIC_REQUEST,
            bootstrap_servers=broker.bootstrap_servers,
            group_id="nl2sql-worker",
            client_id="nl2sql-worker-consumer",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda v: v.decode("utf-8") if v else "",
            max_poll_interval_ms=600000,
            request_timeout_ms=5000,
        )
        while running[0]:
            records = consumer.poll(timeout_ms=1000, max_records=10)
            for tp, batch in records.items():
                for record in batch:
                    if not running[0]:
                        break
                    try:
                        msg = TaskMessage.from_json(record.value)
                        handle_task(msg)
                        consumer.commit()
                    except Exception as e:
                        _log.error("Unhandled error in task %s: %s",
                                   record.key.decode() if record.key else "?", e)
        consumer.close()
    except Exception as e:
        _log.error("Kafka consumer failed: %s", e)
        sys.exit(1)

    broker.close()
    _log.info("Worker stopped")


if __name__ == "__main__":
    main()
