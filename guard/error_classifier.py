from retrieval.schema import get_schema_summary, get_sample_rows, _get_cached_schema_info
from agent.generator_llm import generate_sql, get_dialect_from_url
from retrieval.rag_retrieve import retrieve, build_prompt_context
from guard.review import review
from tools.sql_executor import execute_sql

MAX_REVIEW_ROUNDS = 2
MAX_EXECUTION_RETRIES = 2


def _build_table_catalog(database_url: str | None = None) -> str:
    """One-line per table fallback — ensures no table is missed by RAG."""
    if not database_url:
        return ""
    info = _get_cached_schema_info(database_url)
    lines = []
    for t_lower, t_info in info.items():
        cols = [c["name"] for c in t_info["columns"]]
        lines.append(f"  {t_info['actual_name']}({', '.join(cols)})")
    return "Tables:\n" + "\n".join(lines)


def _build_sample_rows(database_url: str | None = None) -> str:
    if not database_url:
        return ""
    info = _get_cached_schema_info(database_url)
    sample_blocks = []
    for t_lower, t_info in info.items():
        sample_blocks.append(get_sample_rows(t_info["actual_name"], n=3))
    return "\n\n".join(sample_blocks)


def run(question: str,
        rag_schema: bool = True,
        rag_domain: bool = True,
        reviewer_on: bool = True,
        k: int = 8,
        database_url: str | None = None,
        db_id: str | None = None) -> dict:
    """Full pipeline (legacy Mini mode): RAG → Generator → Reviewer → Executor.

    db_id is required for BIRD RAG filtering. Without it, schema retrieval
    falls back to live introspection only (no domain knowledge).
    """
    # ── RAG ──
    rag_chunks = []
    notes_text = ""
    schema_text = ""

    if rag_schema or rag_domain:
        if db_id:
            from retrieval.rag_retrieve import retrieve_bird
            rag_chunks = retrieve_bird(question, db_id, k=k)
        else:
            rag_chunks = retrieve(question, k=k)
        ctx = build_prompt_context(rag_chunks)
        if rag_schema:
            schema_text = ctx["schema_text"]
        if rag_domain:
            notes_text = ctx["notes_text"]

    if not schema_text:
        schema_text = get_schema_summary(database_url)
    else:
        catalog = _build_table_catalog(database_url)
        schema_text = schema_text + "\n\n" + catalog

    sample_rows_text = _build_sample_rows(database_url)

    # ── Generator ──
    _dialect = get_dialect_from_url(database_url) if database_url else None
    token_usage = {"prompt": 0, "completion": 0, "total": 0}
    sql, raw, tu = generate_sql(
        schema_text=schema_text,
        user_question=question,
        rag_context=notes_text,
        sample_rows_text=sample_rows_text,
        dialect=_dialect,
    )
    token_usage = {k: token_usage[k] + tu[k] for k in token_usage}

    # ── Reviewer loop (M3) ──
    review_rounds = []
    if reviewer_on and sql:
        for round_idx in range(MAX_REVIEW_ROUNDS):
            result = review(sql, schema_text, notes_text)
            review_rounds.append(result)

            # Accumulate token usage from Reviewer LLM call
            rev_tu = result.get("token_usage", {})
            if rev_tu:
                token_usage = {k: token_usage[k] + rev_tu.get(k, 0) for k in token_usage}

            if result.get("valid"):
                break

            # Feed back to Generator with review issues
            feedback = _format_review_feedback(result)
            sql, raw, tu = generate_sql(
                schema_text=schema_text,
                user_question=question,
                rag_context=notes_text,
                sample_rows_text=sample_rows_text,
                last_error=feedback,
                last_sql=sql,
                dialect=_dialect,
            )
            if tu:
                token_usage = {k: token_usage[k] + tu[k] for k in token_usage}
            if not sql:
                break

    # ── Executor + Self-Correction loop (M4) ──
    exec_result = None
    exec_attempts = []
    expanded_k = k
    if sql:
        for attempt in range(1 + MAX_EXECUTION_RETRIES):  # initial + retries
            exec_result = execute_sql(sql, database_url=database_url)
            exec_attempts.append({"attempt": attempt + 1, "sql": sql, **exec_result})

            if exec_result["success"]:
                break

            # Self-Correction: feed execution error back to Generator
            if attempt < MAX_EXECUTION_RETRIES:
                hint_type, _ = _classify_exec_error(exec_result.get("error", ""))

                # P2: expand RAG context when column/table is missing
                if hint_type in ("missing_column", "missing_table") and expanded_k == k:
                    expanded_k = k * 2
                    if db_id:
                        from retrieval.rag_retrieve import retrieve_bird
                        expanded_chunks = retrieve_bird(question, db_id, k=expanded_k)
                    else:
                        expanded_chunks = retrieve(question, k=expanded_k)
                    expanded_ctx = build_prompt_context(expanded_chunks)
                    if rag_schema and expanded_ctx["schema_text"]:
                        schema_text = expanded_ctx["schema_text"] + "\n\n" + _build_table_catalog(database_url)
                    if rag_domain and expanded_ctx["notes_text"]:
                        notes_text = expanded_ctx["notes_text"]
                    rag_chunks = expanded_chunks

                error_feedback = _format_exec_error(exec_result, sql)
                sql, raw, tu = generate_sql(
                    schema_text=schema_text,
                    user_question=question,
                    rag_context=notes_text,
                    sample_rows_text=sample_rows_text,
                    last_error=error_feedback,
                    last_sql=sql,
                    dialect=_dialect,
                )
                if tu:
                    token_usage = {k: token_usage[k] + tu[k] for k in token_usage}
                if not sql:
                    break

    return {
        "sql": sql,
        "raw_response": raw,
        "schema_text": schema_text,
        "notes_text": notes_text,
        "rag_chunks": rag_chunks,
        "review_rounds": review_rounds,
        "exec_result": exec_result,
        "exec_attempts": exec_attempts,
        "rag_k_initial": k,
        "rag_k_expanded": expanded_k if expanded_k != k else None,
        "token_usage": token_usage,
    }


def _format_review_feedback(result: dict) -> str:
    parts = ["Reviewer found issues with your SQL:"]
    for issue in result.get("issues", []):
        parts.append(f"- [{issue.get('type', '?')}] {issue.get('detail', '')}")
    fix = result.get("suggested_fix", "")
    if fix:
        parts.append(f"\nSuggested fix: {fix}")
    return "\n".join(parts)


def classify_exec_error(error_msg: str) -> tuple[str, str]:
    """Classify execution error and return (ErrorType, hint_message).

    Recognises SQLite, MySQL, and PostgreSQL error message patterns.
    """
    import re
    from guard.error_types import ErrorType
    lower = error_msg.lower()

    # ── Missing column ──
    if "no such column:" in lower:
        col = error_msg.split("no such column:")[-1].strip()
        return (ErrorType.MISSING_COLUMN,
            f"Column '{col}' does not exist. Check the SCHEMA section — "
            f"only use column names listed there.")
    if "unknown column" in lower:
        col = error_msg.split("Unknown column")[-1].strip(" '\":;")
        col = col.split("'")[1] if "'" in col else col[:40]
        return (ErrorType.MISSING_COLUMN,
            f"Column '{col}' does not exist in the database. "
            f"Check the SCHEMA section for the correct column name.")
    if "column" in lower and "does not exist" in lower:
        m = re.search(r'column\s+"?([^\s"]+)"?\s+does not exist', lower)
        col = m.group(1) if m else "?"
        return (ErrorType.MISSING_COLUMN,
            f"Column '{col}' does not exist. Check the SCHEMA section — "
            f"only use column names listed there.")

    # ── Missing table ──
    if "no such table:" in lower:
        tbl = error_msg.split("no such table:")[-1].strip()
        return (ErrorType.MISSING_TABLE,
            f"Table '{tbl}' does not exist. Check the SCHEMA section for the correct table name.")
    if "doesn't exist" in lower and "table" in lower:
        tbl = error_msg.split("Table")[-1].split("'")[1] if "'" in error_msg else "?"
        return (ErrorType.MISSING_TABLE,
            f"Table '{tbl}' does not exist. Check the SCHEMA section for the correct table name.")
    if "relation" in lower and "does not exist" in lower:
        m = re.search(r'relation\s+"?([^\s"]+)"?\s+does not exist', lower)
        tbl = m.group(1) if m else "?"
        return (ErrorType.MISSING_TABLE,
            f"Table/relation '{tbl}' does not exist. Check the SCHEMA section.")

    # ── Ambiguous column ──
    if "ambiguous column name:" in lower:
        col = error_msg.split("ambiguous column name:")[-1].strip()
        return (ErrorType.AMBIGUOUS_COLUMN,
            f"Column '{col}' exists in multiple tables. "
            f"Qualify it with the table name or alias (e.g., orders.{col}).")
    if "column" in lower and "is ambiguous" in lower:
        return (ErrorType.AMBIGUOUS_COLUMN,
            "A column reference is ambiguous — qualify it with the table name or alias.")

    # ── Bad function ──
    if "no such function:" in lower:
        func = error_msg.split("no such function:")[-1].strip()
        return (ErrorType.BAD_FUNCTION,
            f"Function '{func}' is not available. Use an equivalent built-in function for your dialect.")
    if "function" in lower and "does not exist" in lower:
        m = re.search(r'function\s+(\S+)\(', lower)
        func = m.group(1) if m else "?"
        return (ErrorType.BAD_FUNCTION,
            f"Function '{func}()' does not exist. Use an equivalent built-in function for your dialect.")

    # ── Syntax error ──
    if "syntax error" in lower or "near" in lower:
        return (ErrorType.SYNTAX_ERROR,
            "SQL syntax error. Check for: missing commas, unmatched parentheses, "
            "incomplete JOIN ... ON clauses, or missing keywords.")
    if "you have an error in your sql syntax" in lower:
        return (ErrorType.SYNTAX_ERROR,
            "SQL syntax error. Check for: missing commas, unmatched parentheses, "
            "incomplete JOIN ... ON clauses, or missing keywords.")

    # ── GROUP BY / aggregate ──
    if "group by" in lower or "aggregate" in lower:
        return (ErrorType.GROUP_BY_ERROR,
            "GROUP BY error: every non-aggregate column in SELECT must appear in GROUP BY.")

    return (ErrorType.EXECUTION_ERROR,
        f"SQL execution failed: {error_msg}\n"
        f"Check column names, table names, JOIN conditions, and syntax."
    )


# Backwards-compatible alias
_classify_exec_error = classify_exec_error


def _format_exec_error(exec_result: dict, sql: str) -> str:
    """Format execution error for Generator self-correction feedback."""
    error_msg = exec_result.get("error", "Unknown execution error")
    hint_type, hint = _classify_exec_error(error_msg)

    header = f"The SQL execution failed with the following error:\n  {error_msg}\n"
    return header + "\n" + hint
