"""Refiner node — formats execution/guard/semantic errors with schema-aware hints."""
import time
import difflib
import re

from guard.error_classifier import _classify_exec_error
from retrieval.rag_retrieve import retrieve, build_prompt_context

from agent.state import AgentState


# ── Schema-aware hint helpers ─────────────────────────────────────────────────

def _parse_schema_identifiers(schema_text: str) -> dict[str, list[str]]:
    """Parse DDL text into {table_name: [col1, col2, ...]}."""
    tables: dict[str, list[str]] = {}
    # Split by CREATE TABLE boundaries
    blocks = re.split(r'\n(?=CREATE\s+TABLE\s+)', schema_text, flags=re.IGNORECASE)
    for block in blocks:
        m = re.search(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"?(\w+)"?', block, re.IGNORECASE)
        if not m:
            continue
        table_name = m.group(1).lower()
        cols: list[str] = []
        # Match column definitions within this block only
        for cm in re.finditer(
            r'^\s*"?(\w+)"?\s+(?:TEXT|INTEGER|INT|REAL|FLOAT|DOUBLE|NUMERIC|DECIMAL|BOOLEAN|DATE|DATETIME|BLOB|VARCHAR|CHAR\b|SERIAL|BIGINT|SMALLINT|TIMESTAMP)',
            block, re.IGNORECASE | re.MULTILINE,
        ):
            col_name = cm.group(1).lower()
            # Skip SQL keywords that look like column names
            if col_name not in ("create", "table", "primary", "foreign", "key", "references", "not", "null", "default", "unique", "check", "index", "constraint"):
                cols.append(col_name)
        tables[table_name] = cols
    return tables


def _find_closest(name: str, candidates: list[str], cutoff: float = 0.5) -> str | None:
    """Find closest matching identifier. Tries suffix match first (e.g. 'order_total' → 'total'),
    then full-string similarity."""
    name_lower = name.lower()
    cand_lower = [c.lower() for c in candidates]

    # Strategy 1: match the last token (after . or _) — handles 'customers.cust_name' → 'name'
    tokens = re.split(r'[._]', name_lower)
    last_token = tokens[-1] if tokens else name_lower
    if last_token != name_lower:
        token_match = _find_closest(last_token, cand_lower, cutoff=cutoff)
        if token_match:
            return token_match

    # Strategy 2: full-string difflib match
    matches = difflib.get_close_matches(name_lower, cand_lower, n=1, cutoff=cutoff)
    return matches[0] if matches else None


def _build_schema_hint(offending: str, schema_text: str) -> str:
    """Given a hallucinated identifier, suggest the closest real column/table from schema."""
    tables = _parse_schema_identifiers(schema_text)
    if not tables:
        return ""

    # Collect all candidates: bare column names + qualified table.column names
    all_cols: list[str] = []
    table_cols: dict[str, list[str]] = {}
    for tbl, cols in tables.items():
        for c in cols:
            all_cols.append(c)
            all_cols.append(f"{tbl}.{c}")
        table_cols[tbl] = cols

    # Try to match
    closest = _find_closest(offending, all_cols, cutoff=0.6)
    if closest:
        return f"Did you mean '{closest}'? Check the SCHEMA section for available columns."
    return f"No column or table named '{offending}' found in the schema. Only use identifiers listed in the SCHEMA section."


# ── Error formatters ──────────────────────────────────────────────────────────

def _format_guard_feedback(issues: list[dict], schema_text: str, sql: str) -> str:
    """Format Guard validation issues with unified Type/Error/Fix/Context structure."""
    error_items = []
    fix_items = []
    ctx_items = []

    for i, issue in enumerate(issues[:3]):
        issue_type = issue.get("type", "?")
        detail = issue.get("detail", "")
        error_items.append(f"[{issue_type}] {detail}")

        if issue_type == "hallucination":
            m = re.search(r"'(\w+\.?\w*)'", detail)
            if m:
                hint = _build_schema_hint(m.group(1), schema_text)
                fix_items.append(hint if hint else "Only use identifiers from the SCHEMA section.")
                continue
            m = re.search(r"Identifier '(\w+)'", detail)
            if m:
                hint = _build_schema_hint(m.group(1), schema_text)
                fix_items.append(hint if hint else "Only use identifiers from the SCHEMA section.")
                continue
            fix_items.append("Only use identifiers that exist in the SCHEMA section.")

        elif issue_type == "safety":
            if "does not contain SELECT" in detail:
                fix_items.append("Start your query with SELECT. Only SELECT queries are allowed.")
            elif "Forbidden keyword" in detail:
                kw = detail.split("Forbidden keyword:")[-1].strip()
                fix_items.append(f"Do NOT use {kw}. Rewrite as a read-only SELECT query.")
            elif "Multiple statements" in detail:
                fix_items.append("Combine statements into a single query using CTE (WITH ... AS ...).")
            else:
                fix_items.append("Ensure the query is a single read-only SELECT statement.")

        elif issue_type == "ast_syntax":
            fix_items.append("Check for missing commas, unmatched parentheses, or incomplete JOIN ... ON clauses.")
            loc = re.search(r'(?:near|at)\s+(.+?)(?:\.|$)', detail)
            if loc:
                ctx_items.append(f"Syntax error near: {loc.group(1).strip()[:80]}")

        elif issue_type == "ast_structure":
            n = re.search(r'(\d+)', detail)
            count = n.group(1) if n else "multiple"
            fix_items.append(f"You wrote {count} separate statements. Use WITH ... AS (CTE) to combine them into a single SELECT.")

        elif issue_type == "ast_forbidden":
            kw = re.search(r'Forbidden statement type:\s*(\w+)', detail)
            kw_str = kw.group(1) if kw else "that statement type"
            fix_items.append(f"Do NOT use {kw_str}. Use only SELECT (or WITH ... SELECT). The database is read-only.")

        else:
            fix_items.append("Review the error and fix the SQL accordingly.")

    lines = [
        "## CORRECTION FEEDBACK",
        "Type: Guard failure",
        "Error: " + (" | ".join(error_items) if len(error_items) == 1 else
                     " ".join(f"\n  {j+1}. {e}" for j, e in enumerate(error_items))),
    ]
    if fix_items:
        if len(fix_items) == 1:
            lines.append(f"Fix: {fix_items[0]}")
        else:
            lines.append("Fix:" + "".join(f"\n  {j+1}. {f}" for j, f in enumerate(fix_items)))
    if ctx_items:
        lines.append(f"Context: {'; '.join(ctx_items)}")
    lines.append(f"\n## FAILED SQL — DO NOT REPEAT")
    lines.append(sql[:600])
    return "\n".join(lines)


def _format_exec_feedback(exec_result: dict, sql: str, schema_text: str) -> str:
    """Format execution error with unified Type/Error/Fix/Context structure."""
    error_msg = exec_result.get("error", "Unknown execution error")
    hint_type, hint = _classify_exec_error(error_msg)

    lines = [
        "## CORRECTION FEEDBACK",
        f"Type: Execution error ({hint_type})",
        f"Error: {error_msg[:300]}",
        f"Fix: {hint}",
    ]

    # Context: schema hints + syntax location
    ctx_parts = []
    if hint_type in ("missing_column", "missing_table"):
        m = re.search(r"no such (?:column|table):\s*'?(\w+\.?\w*)'?", error_msg)
        if m:
            schema_hint = _build_schema_hint(m.group(1), schema_text)
            if schema_hint:
                ctx_parts.append(schema_hint)

    if hint_type == "syntax_error":
        near_match = re.search(r'near\s+"([^"]+)"', error_msg)
        if near_match:
            token = near_match.group(1)
            idx = sql.find(token)
            if idx > 0:
                start = max(0, idx - 40)
                end = min(len(sql), idx + len(token) + 40)
                ctx_parts.append(f"Near: ...{sql[start:end]}...")

    if ctx_parts:
        lines.append(f"Context: {'; '.join(ctx_parts)}")

    lines.append(f"\n## FAILED SQL — DO NOT REPEAT")
    lines.append(sql[:600])
    return "\n".join(lines)


def _format_semantic_feedback(feedback: str, sql: str, exec_result: dict) -> str:
    """Format semantic check failure with unified Type/Error/Fix/Context structure."""
    lines = [
        "## CORRECTION FEEDBACK",
        "Type: Semantic error",
        f"Error: {feedback}",
        "Fix: Revise the SQL to correctly answer the user question. The syntax is valid but the query logic or returned data is wrong.",
    ]

    # Context: current execution result preview
    ctx_parts = []
    row_count = exec_result.get("row_count", 0)
    columns = exec_result.get("columns", [])
    data = exec_result.get("data", []) or []
    if columns:
        ctx_parts.append(f"Returned {row_count} rows, columns: {', '.join(str(c) for c in columns[:8])}")
        if data:
            preview = " | ".join(", ".join(str(v)[:20] for v in row[:5]) for row in data[:3])
            ctx_parts.append(f"Preview: {preview}")
    if ctx_parts:
        lines.append(f"Context: {'; '.join(ctx_parts)}")

    lines.append(f"\n## FAILED SQL — DO NOT REPEAT")
    lines.append(sql[:600])
    return "\n".join(lines)


# ── Main node ─────────────────────────────────────────────────────────────────

def refiner_node(state: AgentState) -> dict:
    """Format error with schema-aware hints and set retry context for Generator."""
    t0 = time.time()
    exec_result = state.get("exec_result", {})
    sql = state.get("last_sql", state.get("sql", ""))
    question = state.get("question", "")
    rag_k = state.get("rag_k", 8)
    rag_schema = state.get("rag_schema", True)
    rag_domain = state.get("rag_domain", True)
    database_url = state.get("database_url")
    schema_text = state.get("schema_text", "")
    max_retries = state.get("max_retries", 2)

    tlog = state.get("tlog")
    if tlog:
        err = exec_result.get("error") or ""
        tlog.node_enter("refiner", {"error": err[:80]})

    # Build retry-aware header
    retry_count = state.get("retry_count", 0)
    attempt = retry_count + 1
    max_attempts = max_retries + 1
    if attempt >= max_attempts:
        header = f"## CORRECTION {attempt}/{max_attempts} (FINAL ATTEMPT — be conservative, prefer simpler queries)\n"
    else:
        header = f"## CORRECTION {attempt}/{max_attempts}\n"

    # Build structured feedback based on error source
    semantic_feedback = state.get("semantic_feedback", "")
    exec_error = exec_result.get("error") if exec_result else None
    guard_issues = state.get("guard_issues", [])

    if semantic_feedback and not exec_error:
        error_feedback = _format_semantic_feedback(semantic_feedback, sql, exec_result)
    elif exec_error:
        error_feedback = _format_exec_feedback(exec_result, sql, schema_text)
    elif guard_issues:
        error_feedback = _format_guard_feedback(guard_issues, schema_text, sql)
    else:
        error_feedback = (
            "## CORRECTION FEEDBACK\n"
            "Type: Unknown error — no execution, guard, or semantic details available.\n"
            f"\n## FAILED SQL — DO NOT REPEAT\n{sql[:600]}"
        )

    # Prepend retry-aware header to feedback
    error_feedback = header + error_feedback

    update = {
        "last_error": error_feedback,
        "last_sql": sql,
    }

    # Expand RAG context on missing column/table errors (unchanged logic)
    exec_error_msg = (exec_result or {}).get("error") or ""
    hint_type, _ = _classify_exec_error(exec_error_msg)
    rag_k_expanded = state.get("rag_k_expanded")

    if (hint_type in ("missing_column", "missing_table") or bool(semantic_feedback)) and rag_k_expanded is None:
        expanded_k = rag_k * 2
        db_id = state.get("db_id", "")
        if db_id:
            from retrieval.rag_retrieve import retrieve_bird
            expanded_chunks = retrieve_bird(question, db_id, k=expanded_k)
        else:
            expanded_chunks = retrieve(question, k=expanded_k)
        expanded_ctx = build_prompt_context(expanded_chunks)
        if rag_schema and expanded_ctx["schema_text"]:
            expanded_ctx["schema_text"] = expanded_ctx["schema_text"] + "\n\n" + _build_expanded_catalog(database_url)
        update["rag_k_expanded"] = expanded_k
        update["rag_chunks"] = expanded_chunks
        update["schema_text"] = expanded_ctx.get("schema_text", state.get("schema_text", ""))
        if rag_domain and expanded_ctx.get("notes_text"):
            update["notes_text"] = expanded_ctx["notes_text"]

    if tlog:
        tlog.node_exit("refiner", {"rag_expanded": rag_k_expanded is not None})

    node_latency = dict(state.get("node_latency", {}))
    node_latency["refiner"] = round(time.time() - t0, 3)
    update["node_latency"] = node_latency

    # Record repair history
    repair_history = list(state.get("repair_history", []))
    error_source = "semantic" if (semantic_feedback and not exec_error) else \
                   "execution" if exec_error else \
                   "guard" if guard_issues else "unknown"
    repair_history.append({
        "attempt": retry_count + 1,
        "error_source": error_source,
        "error_type": hint_type if exec_error else "",
        "failed_sql": sql[:300],
        "fix_strategy": "rag_expand" if rag_k_expanded else "feedback_only",
    })
    update["repair_history"] = repair_history

    return update


def _build_expanded_catalog(database_url: str | None = None) -> str:
    if not database_url:
        return ""
    from retrieval.schema import _get_cached_schema_info
    info = _get_cached_schema_info(database_url)
    lines = []
    for t_lower, t_info in info.items():
        cols = [c["name"] for c in t_info["columns"]]
        lines.append(f"  {t_info['actual_name']}({', '.join(cols)})")
    return "Tables:\n" + "\n".join(lines)
