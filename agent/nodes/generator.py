"""Generator node — calls LLM to generate SQL. Supports multi-candidate with dedup."""
import time

from collections.abc import Callable
from agent.state import AgentState

# Module-level token streaming callback. Mutable container to avoid import binding issues.
_stream_ctx: dict = {}


def set_token_callback(cb: Callable[[str], None] | None):
    if cb:
        _stream_ctx["cb"] = cb
    else:
        _stream_ctx.pop("cb", None)


def _get_token_callback() -> Callable[[str], None] | None:
    return _stream_ctx.get("cb")
from storage.config import Config


def _normalize_sql(sql: str) -> str:
    """Normalize whitespace for dedup comparison."""
    return " ".join(sql.split()).strip().upper()


def _generate_one(
    question: str,
    schema_text: str,
    notes_text: str,
    sample_rows_text: str,
    last_error: str,
    last_sql: str,
    sub_questions: list[dict] | None,
    temperature: float,
    fewshot_text: str = "",
    dialect: str | None = None,
    token_callback: Callable[[str], None] | None = None,
) -> tuple[str, str, dict, float]:
    """Generate a single SQL at a given temperature. Supports token-level streaming."""
    from langchain_core.messages import SystemMessage, HumanMessage
    from storage.config import Config
    from agent.generator_llm import SYSTEM_PROMPT, _format_sub_questions, extract_sql, _get_dialect_rules
    from agent.llm_factory import get_llm

    _dialect = (dialect or Config.SQL_DIALECT).upper()

    chat = get_llm(
        temperature=temperature,
        request_timeout=45,
        max_retries=0,
        streaming=token_callback is not None,
    )

    system = SYSTEM_PROMPT.format(
        dialect=_dialect,
        dialect_rules=_get_dialect_rules(_dialect),
    )

    parts = []
    # On retry, put error context FIRST so the model knows it's in correction mode
    if last_error and last_sql:
        parts.append(f"## LAST_ERROR\n{last_error}")
        parts.append(f"## LAST_SQL\n{last_sql}")
    parts.append(f"## SCHEMA\n{schema_text}")
    if fewshot_text:
        parts.append(f"## FEW-SHOT EXAMPLES\n{fewshot_text}")
    if sample_rows_text:
        parts.append(f"## SAMPLE ROWS\n{sample_rows_text}")
    if notes_text:
        parts.append(
            "## RETRIEVED NOTES\n"
            "Notes are hints only. SCHEMA is the ground truth — if notes conflict "
            "with schema columns, types, or values, trust the schema.\n"
            f"{notes_text}"
        )
    if sub_questions:
        parts.append(f"## SUB_QUESTIONS (use CTE/WITH to implement each step)\n{_format_sub_questions(sub_questions)}")
    parts.append(f"## USER QUESTION\n{question}")

    messages = [
        SystemMessage(content=system),
        HumanMessage(content="\n\n".join(parts)),
    ]

    if token_callback:
        _t0 = time.time()
        raw = ""
        token_usage = {}
        for chunk in chat.stream(messages):
            content = chunk.content
            if content:
                raw += content
                token_callback(content)
            # DeepSeek streaming: token_usage is in chunk.usage_metadata, not response_metadata
            tu = None
            if hasattr(chunk, "response_metadata") and chunk.response_metadata:
                tu = chunk.response_metadata.get("token_usage")
            if not tu and hasattr(chunk, "usage_metadata") and chunk.usage_metadata:
                um = chunk.usage_metadata
                tu = {"prompt_tokens": um.get("input_tokens", 0),
                      "completion_tokens": um.get("output_tokens", 0),
                      "total_tokens": um.get("total_tokens", 0)}
            if tu:
                token_usage = {
                    "prompt": tu.get("prompt_tokens", 0),
                    "completion": tu.get("completion_tokens", 0),
                    "total": tu.get("total_tokens", 0),
                }
        _duration = round(time.time() - _t0, 3)
        if not token_usage:
            token_usage = {"prompt": 0, "completion": 0, "total": 0}
    else:
        _t0 = time.time()
        response = chat.invoke(messages)
        _duration = round(time.time() - _t0, 3)
        raw = response.content
        tu = response.response_metadata.get("token_usage", {}) if hasattr(response, "response_metadata") else {}
        token_usage = {
            "prompt": tu.get("prompt_tokens", 0),
            "completion": tu.get("completion_tokens", 0),
            "total": tu.get("total_tokens", 0),
        }

    sql = extract_sql(raw)
    return sql, raw, token_usage, _duration


def generator_node(state: AgentState) -> dict:
    """Generate SQL. Multi-candidate with dedup + early stop when enabled."""
    from agent.generator_llm import get_dialect_from_url

    t0 = time.time()
    question = state["question"]
    tlog = state.get("tlog")
    if tlog:
        tlog.node_enter("generator", {"question_len": len(question)})

    schema_text = state.get("schema_text", "")
    notes_text = state.get("notes_text", "")
    sample_rows_text = state.get("sample_rows_text", "")
    last_error = state.get("last_error", "")
    last_sql = state.get("last_sql", "")
    sub_questions = state.get("sub_questions") or None
    fewshot_text = state.get("fewshot_text", "")
    multi = state.get("retry_count", 0) > 0
    database_url = state.get("database_url")
    _dialect = get_dialect_from_url(database_url) if database_url else None

    token_usage = state.get("token_usage", {"prompt": 0, "completion": 0, "total": 0})
    token_callback = _get_token_callback()

    if not multi:
        try:
            sql, raw, tu, dur = _generate_one(
                question, schema_text, notes_text, sample_rows_text,
                last_error, last_sql, sub_questions, temperature=0,
                fewshot_text=fewshot_text, dialect=_dialect,
                token_callback=token_callback,
            )
        except Exception as e:
            if tlog:
                tlog.llm_error(Config.LLM_CHAT_MODEL, type(e).__name__, str(e)[:300], node="generator")
                tlog.node_exit("generator", {"error": str(e)[:120]}, status="error")
            raise
        token_usage = {k: token_usage[k] + tu.get(k, 0) for k in token_usage}
        node_latency = dict(state.get("node_latency", {}))
        node_latency["generator"] = round(time.time() - t0, 3)
        if tlog:
            tlog.llm_call(Config.LLM_CHAT_MODEL, tu, dur, node="generator")
            tlog.node_exit("generator", {"sql_len": len(sql), "candidate_count": 1}, status="success")
        return {
            "sql": sql,
            "raw_response": raw,
            "token_usage": token_usage,
            "candidate_sqls": [],
            "last_error": "",
            "last_sql": "",
            "node_latency": node_latency,
        }

    # Multi-candidate: temp 0, 0.3, 0.6 with dedup + early stop.
    # Only stream tokens for the first candidate (temp=0).
    temperatures = [0, 0.3, 0.6]
    candidates: list[str] = []
    raw_responses: list[str] = []

    for i, temp in enumerate(temperatures):
        # Only stream tokens for the first candidate
        cb = token_callback if i == 0 else None
        try:
            sql, raw, tu, dur = _generate_one(
                question, schema_text, notes_text, sample_rows_text,
                last_error, last_sql, sub_questions, temperature=temp,
                fewshot_text=fewshot_text, dialect=_dialect,
                token_callback=cb,
            )
        except Exception as e:
            if tlog:
                tlog.llm_error(Config.LLM_CHAT_MODEL, type(e).__name__, str(e)[:300], node="generator")
            if i == 0:
                # First candidate failed — re-raise to trigger graph-level error handling
                if tlog:
                    tlog.node_exit("generator", {"error": str(e)[:120]}, status="error")
                raise
            # Non-first candidate failed — skip and continue
            continue
        token_usage = {k: token_usage[k] + tu.get(k, 0) for k in token_usage}
        raw_responses.append(raw)
        if tlog:
            tlog.llm_call(Config.LLM_CHAT_MODEL, tu, dur, node="generator")

        if not sql:
            continue

        norm = _normalize_sql(sql)
        # Check duplicate against earlier candidates
        is_dup = any(_normalize_sql(c) == norm for c in candidates)

        if not is_dup:
            candidates.append(sql)

        # Early stop: first temp=0 → if temp=0.3 same, skip temp=0.6
        if len(temperatures) > 2 and len(candidates) >= 1 and temp == temperatures[1] and is_dup:
            break

    if not candidates:
        # All failed → return empty, will be caught downstream
        node_latency = dict(state.get("node_latency", {}))
        node_latency["generator"] = round(time.time() - t0, 3)
        if tlog:
            tlog.node_exit("generator", {"sql_len": 0, "candidate_count": 0, "error": "all failed"})
        return {
            "sql": "",
            "raw_response": "\n---\n".join(raw_responses),
            "token_usage": token_usage,
            "candidate_sqls": [],
            "last_error": "All generation attempts returned empty SQL.",
            "last_sql": "",
            "node_latency": node_latency,
        }

    node_latency = dict(state.get("node_latency", {}))
    node_latency["generator"] = round(time.time() - t0, 3)

    if tlog:
        tlog.node_exit("generator", {"sql_len": len(candidates[0]), "candidate_count": len(candidates)})

    return {
        "sql": candidates[0],  # primary SQL (temp=0 or first unique)
        "raw_response": "\n---\n".join(raw_responses),
        "token_usage": token_usage,
        "candidate_sqls": candidates,
        "last_error": "",
        "last_sql": "",
        "node_latency": node_latency,
    }
