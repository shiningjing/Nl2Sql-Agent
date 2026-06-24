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
import threading
import time
import traceback
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError

# Ensure project root is on path (for python -m worker.main)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from infrastructure.broker import (
    TOPIC_REQUEST, TOPIC_STATUS, TOPIC_RESULT, TOPIC_DLQ, TOPIC_FEEDBACK,
    TaskMessage, get_broker,
)
from infrastructure.task_store import (
    task_create, task_get, task_update, task_transition,
    task_is_cancelled, task_clear_cancel, feedback_transition,
    task_heartbeat, task_publish_token, get_task_timeout,
    HEARTBEAT_INTERVAL_S,
)

_log = logging.getLogger("nl2sql.worker")
MAX_RETRIES = 3


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

    # Inject token callback for real-time SQL streaming via Redis pub/sub
    import agent.nodes.generator as gen_mod

    def _on_token(text: str):
        task_publish_token(task_id, text)

    gen_mod.set_token_callback(_on_token)

    try:
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
    finally:
        gen_mod.set_token_callback(None)

    elapsed = round(time.time() - t0, 2)
    accumulated["_worker_elapsed"] = elapsed

    # Extract final results
    sql = accumulated.get("sql") or accumulated.get("chosen_sql", "")
    exec_result = accumulated.get("exec_result")

    task_update(task_id, sql=sql, exec_result=_sanitize(exec_result),
                token_usage=accumulated.get("token_usage", {}),
                node_timings=accumulated.get("node_latency", {}),
                # Persist context for feedback rounds
                _schema_text=accumulated.get("schema_text", ""),
                _notes_text=accumulated.get("notes_text", ""),
                _fewshot_text=accumulated.get("fewshot_text", ""),
                _original_payload=payload)

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


# ── Feedback graph runner ─────────────────────────────────────────────────────

def run_feedback_graph(task_id: str, payload: dict) -> dict:
    """Execute a human-feedback correction round using the feedback sub-graph.

    Loads preserved schema/few-shot context from Redis, injects user_feedback
    into state, and runs Refiner → Generator → Guard → Voter → SemCheck.
    """
    from agent.graphs.feedback_graph import create_feedback_graph

    # Load preserved context from Redis (set during initial run_graph)
    task_state = task_get(task_id) or {}
    db_id = payload.get("db_id", task_state.get("db_id", ""))
    database_url = payload.get("database_url", task_state.get("database_url", ""))

    initial_state = {
        # Core context from original task
        "question": payload.get("question", task_state.get("question", "")),
        "db_id": db_id,
        "database_url": database_url,
        "schema_text": task_state.get("_schema_text", ""),
        "notes_text": task_state.get("_notes_text", ""),
        "fewshot_text": task_state.get("_fewshot_text", ""),
        # Previous result (for Refiner context)
        "sql": payload.get("sql", task_state.get("sql", "")),
        "last_sql": payload.get("sql", task_state.get("sql", "")),
        "exec_result": payload.get("exec_result") or task_state.get("exec_result"),
        # User feedback (Refiner reads this first)
        "user_feedback": payload.get("feedback", ""),
        # Conversation history
        "conversation_turns": payload.get("conversation_turns", []),
        # Correction control
        "retry_count": 0,
        "max_retries": 2,
        "is_feedback_round": True,
        # RAG / fewshot flags
        "rag_schema": payload.get("rag_schema", True),
        "rag_domain": payload.get("rag_domain", True),
        "rag_k": payload.get("rag_k", 8),
        "fewshot_enabled": payload.get("fewshot_enabled", True),
        "multi_candidate": payload.get("multi_candidate", True),
    }

    graph = create_feedback_graph()
    t0 = time.time()
    broker = get_broker()
    accumulated = dict(initial_state)

    import agent.nodes.generator as gen_mod

    def _on_token(text: str):
        task_publish_token(task_id, text)

    gen_mod.set_token_callback(_on_token)

    try:
        for step in graph.stream(initial_state, stream_mode="updates"):
            if task_is_cancelled(task_id):
                _log.info("Task %s (feedback) cancelled mid-execution", task_id)
                task_transition(task_id, "CANCELLED")
                broker.publish(TOPIC_STATUS, TaskMessage(
                    task_id=task_id, event="cancelled",
                    payload={"node": "feedback"},
                ))
                task_clear_cancel(task_id)
                return {"_cancelled": True}

            for node_name, node_output in step.items():
                if isinstance(node_output, dict):
                    accumulated.update(node_output)

                summary = _summarize_node(node_name, accumulated)
                broker.publish(TOPIC_STATUS, TaskMessage(
                    task_id=task_id, event="node_done",
                    payload={"node": node_name, "summary": summary},
                ))

                task_update(task_id, node=node_name, progress=_node_progress(node_name),
                            sql=accumulated.get("sql") or accumulated.get("chosen_sql"),
                            token_usage=accumulated.get("token_usage", {}),
                            node_timings=accumulated.get("node_latency", {}))
    finally:
        gen_mod.set_token_callback(None)

    elapsed = round(time.time() - t0, 2)
    sql = accumulated.get("sql") or accumulated.get("chosen_sql", "")
    exec_result = accumulated.get("exec_result")

    # Append this turn to conversation history
    conversation_turns = list(initial_state.get("conversation_turns", []))
    conversation_turns.append({
        "turn": len(conversation_turns) + 1,
        "user_feedback": payload.get("feedback", ""),
        "sql": sql,
        "exec_result": _sanitize(exec_result),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })

    task_update(task_id, sql=sql, exec_result=_sanitize(exec_result),
                token_usage=accumulated.get("token_usage", {}),
                node_timings=accumulated.get("node_latency", {}),
                conversation_turns=conversation_turns,
                user_feedback="")  # Clear for next round

    broker.publish(TOPIC_RESULT, TaskMessage(
        task_id=task_id, event="success",
        payload={
            "sql": sql,
            "exec_result": _sanitize(exec_result),
            "token_usage": accumulated.get("token_usage", {}),
            "node_timings": accumulated.get("node_latency", {}),
            "elapsed_s": elapsed,
            "turn": len(conversation_turns),
        },
    ))

    return accumulated


def handle_feedback(msg: TaskMessage) -> None:
    """Kafka callback for feedback messages — run a correction round."""
    task_id = msg.task_id
    payload = msg.payload or {}
    turn = payload.get("turn", 1)

    # Skip if already RUNNING (duplicate submit prevention)
    existing = task_get(task_id)
    if existing and existing.get("status") == "RUNNING":
        _log.warning("Task %s already RUNNING — skipping duplicate feedback", task_id)
        return

    feedback_transition(task_id)
    broker = get_broker()
    broker.publish(TOPIC_STATUS, TaskMessage(
        task_id=task_id, event="running",
        payload={"source": "feedback", "turn": turn},
    ))

    heartbeat_stop = threading.Event()

    def _heartbeat_loop():
        while not heartbeat_stop.is_set():
            task_heartbeat(task_id)
            heartbeat_stop.wait(HEARTBEAT_INTERVAL_S)

    hb_thread = threading.Thread(target=_heartbeat_loop, daemon=True, name=f"hb-{task_id}")
    hb_thread.start()

    try:
        pool = ThreadPoolExecutor(max_workers=1)
        try:
            fut = pool.submit(run_feedback_graph, task_id, payload)
            result = fut.result(timeout=get_task_timeout(0))
            if not result.get("_cancelled"):
                task_transition(task_id, "SUCCESS")
        except FutureTimeoutError:
            _log.warning("Feedback turn %d for task %s timed out", turn, task_id)
            task_transition(task_id, "TIMEOUT",
                            error=f"Feedback turn {turn} timed out")
            broker.publish(TOPIC_STATUS, TaskMessage(
                task_id=task_id, event="timeout",
                payload={"source": "feedback", "turn": turn},
            ))
        finally:
            pool.shutdown(wait=False)
    except Exception as e:
        err_msg = f"{type(e).__name__}: {str(e)[:300]}"
        _log.error("Feedback turn %d for task %s failed: %s", turn, task_id, err_msg)
        task_transition(task_id, "FAILED", error=err_msg)
        broker.publish(TOPIC_STATUS, TaskMessage(
            task_id=task_id, event="failed",
            payload={"error": err_msg, "source": "feedback", "turn": turn},
        ))
    finally:
        heartbeat_stop.set()
        hb_thread.join(timeout=2)


# ── Message handler ──────────────────────────────────────────────────────────

def handle_task(msg: TaskMessage) -> None:
    """Callback from Kafka consumer — run the task pipeline with heartbeat and timeout."""
    task_id = msg.task_id
    payload = msg.payload
    retry_count = payload.get("_retry_count", 0)
    timeout_s = get_task_timeout(retry_count)

    # Check if cancelled before starting
    if task_is_cancelled(task_id):
        task_transition(task_id, "CANCELLED")
        _log.info("Task %s cancelled before start", task_id)
        return

    task_transition(task_id, "RUNNING", retry_count=retry_count)
    broker = get_broker()
    broker.publish(TOPIC_STATUS, TaskMessage(
        task_id=task_id, event="running",
        payload={"retry_count": retry_count, "timeout_s": timeout_s},
    ))

    # Background heartbeat thread — keeps ticking even if Graph/SQL blocks
    heartbeat_stop = threading.Event()

    def _heartbeat_loop():
        while not heartbeat_stop.is_set():
            task_heartbeat(task_id)
            heartbeat_stop.wait(HEARTBEAT_INTERVAL_S)

    hb_thread = threading.Thread(target=_heartbeat_loop, daemon=True, name=f"hb-{task_id}")
    hb_thread.start()

    try:
        pool = ThreadPoolExecutor(max_workers=1)
        try:
            fut = pool.submit(run_graph, task_id, payload)
            result = fut.result(timeout=timeout_s)
            # If cancelled mid-execution, run_graph already set CANCELLED; don't overwrite
            if result.get("_cancelled"):
                _log.info("Task %s was cancelled — not marking SUCCESS", task_id)
            else:
                task_transition(task_id, "SUCCESS")
        except FutureTimeoutError:
            _log.warning("Task %s timed out after %ds (retry %d)",
                        task_id, timeout_s, retry_count)
            if task_is_cancelled(task_id):
                task_transition(task_id, "CANCELLED", error="Cancelled during execution")
                task_clear_cancel(task_id)
            else:
                task_transition(task_id, "TIMEOUT",
                                error=f"Task timed out after {timeout_s}s")
                broker.publish(TOPIC_STATUS, TaskMessage(
                    task_id=task_id, event="timeout",
                    payload={"retry_count": retry_count, "timeout_s": timeout_s},
                ))
        finally:
            pool.shutdown(wait=False)
    except Exception as e:
        err_msg = f"{type(e).__name__}: {str(e)[:300]}"
        _log.error("Task %s failed (attempt %d/%d): %s",
                   task_id, retry_count + 1, MAX_RETRIES + 1, err_msg)

        if task_is_cancelled(task_id):
            # Cancelled mid-execution — don't retry
            _log.info("Task %s was cancelled — skipping retry", task_id)
            task_transition(task_id, "CANCELLED",
                            error=f"Cancelled after error: {err_msg}")
            task_clear_cancel(task_id)
        elif retry_count < MAX_RETRIES:
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
    finally:
        heartbeat_stop.set()
        hb_thread.join(timeout=2)


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

    _log.info("Worker listening on topics: %s, %s", TOPIC_REQUEST, TOPIC_FEEDBACK)

    # Kafka consumer loop runs in foreground; signal handler sets running=False
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            TOPIC_REQUEST, TOPIC_FEEDBACK,
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
                        if record.topic == TOPIC_FEEDBACK:
                            handle_feedback(msg)
                        else:
                            handle_task(msg)
                        consumer.commit()
                    except Exception as e:
                        _log.error("Unhandled error in topic %s task %s: %s",
                                   record.topic, record.key.decode() if record.key else "?", e)
        consumer.close()
    except Exception as e:
        _log.error("Kafka consumer failed: %s", e)
        sys.exit(1)

    broker.close()
    _log.info("Worker stopped")


if __name__ == "__main__":
    main()
