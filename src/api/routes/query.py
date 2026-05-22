"""NL2SQL query endpoints — Mini pipeline and Full Graph."""
import asyncio
import concurrent.futures
import json
import time

from fastapi import APIRouter, Request
from sse_starlette.sse import EventSourceResponse

from src.api.models import (
    QueryRequest,
    QueryFullRequest,
    QueryResponse,
    ExecResult,
)
from src.infrastructure.redis_cache import cache_get_llm, cache_set_llm
from src.infrastructure.llm_factory import set_llm_config, clear_llm_config
from src.obs.logger import TraceLogger
from nl2sql.execute import execute_sql

router = APIRouter()


def _apply_llm_config(llm) -> None:
    """Set per-request LLM config from request model. No-op if llm is None or all fields are None."""
    if llm and any((llm.model, llm.api_key, llm.base_url)):
        set_llm_config(
            model=llm.model,
            api_key=llm.api_key,
            base_url=llm.base_url or "",
        )


def _sanitize_exec_result(er: dict) -> dict:
    """Convert SQLAlchemy Row objects + Decimal types to JSON-safe plain types."""
    from decimal import Decimal

    def _safe(v):
        if isinstance(v, Decimal):
            return float(v)
        return v

    data = er.get("data")
    if data:
        er = {**er, "data": [tuple(_safe(v) for v in row) for row in data]}
    return er


def _verify_cache(sql: str, database_url: str | None = None) -> ExecResult | None:
    """Re-execute cached SQL to verify it's still valid. Returns None if invalid."""
    er = execute_sql(sql, database_url=database_url)
    if er["success"] and er.get("row_count", 0) > 0:
        return ExecResult(**_sanitize_exec_result(er))
    return None


def _get_tlog(request: Request) -> TraceLogger:
    """Get TraceLogger with the trace_id from middleware (or generate)."""
    trace_id = getattr(request.state, "trace_id", "")
    return TraceLogger(trace_id if trace_id else None)


@router.post("/query", response_model=QueryResponse)
def query_nl2sql(req: QueryRequest, request: Request):
    """Mini pipeline: RAG → Generator → Reviewer → Executor → Self-Correction."""
    tlog = _get_tlog(request)
    _apply_llm_config(req.llm)
    start = time.time()

    # ── Check cache ──
    if req.use_cache:
        cached = cache_get_llm(req.question)
        if cached:
            verified = _verify_cache(cached["sql"], req.database_url)
            if verified is not None:
                elapsed_ms = (time.time() - start) * 1000
                tlog.cache_hit(cached["similarity"], cached["question"])
                tlog.api_request("end", method="POST", path="/api/v1/query",
                                 status=200, elapsed_ms=elapsed_ms)
                return QueryResponse(
                    request_id=tlog.trace_id,
                    question=req.question,
                    sql=cached["sql"],
                    exec_result=verified,
                    cache_hit=True,
                    elapsed_ms=elapsed_ms,
                )
            tlog.cache_miss("exec_verify_failed")

    # ── Run pipeline ──
    from nl2sql.pipeline import run

    tlog._emit("pipeline_start", {"mode": "mini", "question": req.question[:100]})

    result = run(
        question=req.question,
        rag_schema=req.rag_schema,
        rag_domain=req.rag_domain,
        reviewer_on=req.reviewer_on,
        k=req.k,
        database_url=req.database_url,
        db_id=req.db_id,
    )

    sql = result.get("sql", "")
    exec_result_raw = result.get("exec_result", {})
    exec_result = None
    if exec_result_raw:
        exec_result_raw = _sanitize_exec_result(exec_result_raw)
        exec_result = ExecResult(**exec_result_raw)

    # ── Store in cache ──
    if req.use_cache and sql and exec_result and exec_result.success:
        cache_set_llm(req.question, sql, exec_result_raw)

    elapsed_ms = (time.time() - start) * 1000

    tlog._emit("pipeline_end", {
        "mode": "mini",
        "sql_ok": bool(sql),
        "exec_ok": exec_result.success if exec_result else False,
        "row_count": exec_result.row_count if exec_result else 0,
        "elapsed_ms": round(elapsed_ms, 1),
    })

    return QueryResponse(
        request_id=tlog.trace_id,
        question=req.question,
        sql=sql,
        exec_result=exec_result,
        token_usage=result.get("token_usage", {}),
        cache_hit=False,
        elapsed_ms=elapsed_ms,
    )


@router.post("/query/full", response_model=QueryResponse)
def query_full_graph(req: QueryFullRequest, request: Request):
    """Full Graph pipeline: Router → SchemaRetriever → Decomposer → Generator → Guard → Voter → Executor."""
    tlog = _get_tlog(request)
    _apply_llm_config(req.llm)
    start = time.time()

    # ── Check cache ──
    if req.use_cache:
        cached = cache_get_llm(req.question)
        if cached:
            verified = _verify_cache(cached["sql"], req.database_url)
            if verified is not None:
                elapsed_ms = (time.time() - start) * 1000
                tlog.cache_hit(cached["similarity"], cached["question"])
                tlog.api_request("end", method="POST", path="/api/v1/query/full",
                                 status=200, elapsed_ms=elapsed_ms)
                return QueryResponse(
                    request_id=tlog.trace_id,
                    question=req.question,
                    sql=cached["sql"],
                    exec_result=verified,
                    cache_hit=True,
                    elapsed_ms=elapsed_ms,
                )
            tlog.cache_miss("exec_verify_failed")

    # ── Run Full Graph ──
    from src.agent.graphs.full_graph import create_full_graph

    tlog._emit("pipeline_start", {"mode": "full", "question": req.question[:100]})

    graph = create_full_graph()
    initial_state = {
        "question": req.question,
        "rag_schema": req.rag_schema,
        "rag_domain": req.rag_domain,
        "multi_candidate": req.multi_candidate,
        "rag_k": req.rag_k,
        "rag_column_prune": req.rag_column_prune,
        "rag_hybrid": req.rag_hybrid,
        "rag_fk_expand": req.rag_fk_expand,
        "fewshot_enabled": req.fewshot_enabled,
        "database_url": req.database_url,
        "db_id": req.db_id,
        "trace_id": tlog.trace_id,  # unify trace_id with middleware
    }
    state = graph.invoke(initial_state)

    sql = state.get("sql", "") or state.get("chosen_sql", "")
    exec_result = None
    er_raw = state.get("exec_result")
    if er_raw:
        er_raw = _sanitize_exec_result(er_raw)
        exec_result = ExecResult(**er_raw)

    # ── Store in cache ──
    if req.use_cache and sql and exec_result and exec_result.success:
        cache_set_llm(req.question, sql, er_raw)

    elapsed_ms = (time.time() - start) * 1000

    tlog._emit("pipeline_end", {
        "mode": "full",
        "sql_ok": bool(sql),
        "exec_ok": exec_result.success if exec_result else False,
        "row_count": exec_result.row_count if exec_result else 0,
        "elapsed_ms": round(elapsed_ms, 1),
    })

    return QueryResponse(
        request_id=tlog.trace_id,
        question=req.question,
        sql=sql,
        exec_result=exec_result,
        token_usage=state.get("token_usage", {}),
        cache_hit=False,
        elapsed_ms=elapsed_ms,
    )


def _summarize_node(node_name: str, state: dict) -> dict:
    """Extract human-readable summary from node state for SSE progress events."""
    summary = {}
    if node_name == "schema_retriever":
        chunks = state.get("rag_chunks", [])
        summary["chunk_count"] = len(chunks)
        summary["schema_len"] = len(state.get("schema_text", ""))
    elif node_name == "router":
        summary["complexity"] = state.get("complexity", "simple")
        summary["score"] = state.get("router_score", 0)
    elif node_name == "decomposer":
        subs = state.get("sub_questions", [])
        summary["sub_count"] = len(subs)
    elif node_name == "fewshot_selector":
        hits = state.get("fewshot_hits", [])
        summary["hit_count"] = len(hits)
    elif node_name == "generator":
        sql = state.get("sql", "")
        candidates = state.get("candidate_sqls", [])
        summary["candidate_count"] = len(candidates) if candidates else 1
        sql_preview = sql[:200] if sql else (candidates[0][:200] if candidates else "")
        summary["sql_preview"] = sql_preview
    elif node_name == "guard":
        summary["guard_pass"] = state.get("guard_pass", False)
        issues = state.get("guard_issues", [])
        summary["issue_count"] = len(issues)
    elif node_name == "voter":
        er = state.get("exec_result", {})
        summary["exec_success"] = er.get("success", False) if er else False
        summary["row_count"] = er.get("row_count", 0) if er else 0
    elif node_name == "semantic_check":
        summary["semantic_pass"] = state.get("semantic_pass", True)
        summary["feedback"] = (state.get("semantic_feedback", "") or "")[:200]
    elif node_name == "refiner":
        summary["retry_count"] = state.get("retry_count", 0)
        summary["last_error"] = (state.get("last_error", "") or "")[:200]
    return summary


@router.post("/query/full/stream")
async def query_full_graph_stream(req: QueryFullRequest, request: Request):
    """Stream full graph pipeline progress via Server-Sent Events."""
    from src.agent.graphs.full_graph import create_full_graph

    initial_state = {
        "question": req.question,
        "db_id": req.db_id,
        "database_url": req.database_url,
        "rag_schema": req.rag_schema,
        "rag_domain": req.rag_domain,
        "multi_candidate": req.multi_candidate,
        "rag_k": req.rag_k,
        "rag_column_prune": req.rag_column_prune,
        "rag_hybrid": req.rag_hybrid,
        "rag_fk_expand": req.rag_fk_expand,
        "fewshot_enabled": req.fewshot_enabled,
    }

    async def event_generator():
        graph = create_full_graph()
        queue: asyncio.Queue = asyncio.Queue()
        loop = asyncio.get_event_loop()

        def make_token_callback():
            def cb(text: str):
                asyncio.run_coroutine_threadsafe(
                    queue.put(("token", text)), loop
                )
            return cb

        def run_graph():
            # Set LLM config inside thread (contextvars don't cross thread-pool boundary)
            _apply_llm_config(req.llm)
            # Set token callback inside the graph thread so generator_node can pick it up
            import src.agent.nodes.generator as gen_mod
            gen_mod.set_token_callback(make_token_callback())
            accumulated = dict(initial_state)
            try:
                for step in graph.stream(initial_state, stream_mode="updates"):
                    asyncio.run_coroutine_threadsafe(queue.put(("node", step)), loop)
                    # Merge each node's output into accumulated state
                    for node_output in step.values():
                        if isinstance(node_output, dict):
                            accumulated.update(node_output)
                asyncio.run_coroutine_threadsafe(queue.put(("done", accumulated)), loop)
            except Exception as e:
                asyncio.run_coroutine_threadsafe(queue.put(("error", str(e))), loop)
            finally:
                gen_mod.set_token_callback(None)
                clear_llm_config()

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        executor.submit(run_graph)

        while True:
            msg = await queue.get()
            event_type, data = msg

            if event_type == "token":
                yield {
                    "event": "token",
                    "data": json.dumps({"text": data}, ensure_ascii=False),
                }
            elif event_type == "node":
                node_name = list(data.keys())[0]
                node_state = data[node_name]
                yield {
                    "event": "node_complete",
                    "data": json.dumps({
                        "node": node_name,
                        "summary": _summarize_node(node_name, node_state),
                    }, default=str, ensure_ascii=False),
                }
            elif event_type == "done":
                final_state = data
                sql = (final_state.get("sql") or final_state.get("chosen_sql") or "")
                exec_result = final_state.get("exec_result")
                yield {
                    "event": "complete",
                    "data": json.dumps({
                        "sql": sql,
                        "exec_result": _sanitize_exec_result(exec_result) if exec_result else None,
                        "token_usage": final_state.get("token_usage", {}),
                        "node_timings": _extract_node_timings(final_state),
                    }, default=str, ensure_ascii=False),
                }
                break
            elif event_type == "error":
                yield {
                    "event": "error",
                    "data": json.dumps({"error": data}, ensure_ascii=False),
                }
                break

        executor.shutdown(wait=False)

    return EventSourceResponse(event_generator())


def _extract_node_timings(state: dict) -> dict:
    """Extract node timings from tlog if present in state."""
    tlog = state.get("tlog")
    if tlog and hasattr(tlog, "get_node_timings"):
        return tlog.get_node_timings()
    return {}
