"""SemanticCheck node — LLM binary YES/NO check on Voter winner.

Addresses W4 blind spot: multi-candidate same-wrong-result passes hash voting.
After Voter picks a winner, this node asks LLM: "Does this SQL correctly answer the question?"
If NO → routes to Refiner with semantic error feedback.
"""

import re
import time

from src.agent.state import AgentState
from nl2sql.config import Config
from src.prompts import SEMANTIC_CHECK_PROMPT as _SEMANTIC_CHECK_PROMPT, SEMANTIC_CHECK_SYSTEM_PROMPT


# ── Constants ──────────────────────────────────────────────────────────────────
_SCHEMA_MAX_CHARS = 8000
_SQL_MAX_CHARS = 6000   # safety ceiling; normal SQL stays well under this
_PREVIEW_VALUE_MAX = 40


# ── Helpers ────────────────────────────────────────────────────────────────────

def _normalize_sql(sql: str) -> str:
    return " ".join(sql.lower().split())


def _extract_sql_tables(sql: str) -> set[str]:
    """Parse table names from SQL (regex; Guard-trusted pattern)."""
    tables: set[str] = set()
    for m in re.finditer(
        r"(?:FROM|JOIN)\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?",
        sql, re.IGNORECASE,
    ):
        tables.add(m.group(1).lower())
    return tables


def _filter_schema_text(schema_text: str, used_tables: set[str], max_chars: int) -> str:
    """Return DDL blocks only for tables referenced in SQL, with a truncation notice
    if the output exceeds max_chars.

    Falls back to full schema_text (with boundary truncation) when used_tables is
    empty or no matching blocks are found.
    """
    if not schema_text:
        return ""

    # Split into per-table DDL blocks
    blocks: list[str] = []
    for part in re.split(r"\n(?=CREATE\s+TABLE\s+)", schema_text):
        part_stripped = part.strip()
        if not part_stripped:
            continue
        blocks.append(part_stripped)

    if not used_tables:
        return _truncate_at_ddl_boundary(schema_text, max_chars)

    # Match blocks to used tables (CREATE TABLE <name> ...)
    relevant: list[str] = []
    unused: list[str] = []
    for blk in blocks:
        m = re.match(r"CREATE\s+TABLE\s+\"?(\w+)\"?", blk, re.IGNORECASE)
        if m and m.group(1).lower() in used_tables:
            relevant.append(blk)
        else:
            unused.append(blk)

    if not relevant:
        return _truncate_at_ddl_boundary(schema_text, max_chars)

    # Join relevant blocks
    filtered = "\n\n".join(relevant)

    # If within limit, add a compact note about omitted tables
    if len(filtered) <= max_chars:
        if unused:
            omitted_names = set()
            for blk in unused:
                m = re.match(r"CREATE\s+TABLE\s+\"?(\w+)\"?", blk, re.IGNORECASE)
                if m:
                    omitted_names.add(m.group(1))
            if omitted_names:
                filtered += (
                    f"\n\n-- Note: {len(omitted_names)} other table(s) in schema but not "
                    f"referenced by this SQL: {', '.join(sorted(omitted_names))}"
                )
        return filtered

    # Need truncation within relevant blocks
    return _truncate_at_ddl_boundary(filtered, max_chars)


def _truncate_at_ddl_boundary(text: str, max_chars: int) -> str:
    """Cut at the last complete CREATE TABLE ... ; boundary within max_chars."""
    if len(text) <= max_chars:
        return text

    truncated = text[:max_chars]
    # Find last ");" (end of CREATE TABLE or FOREIGN KEY line) within the window
    last_end = truncated.rfind(");")
    if last_end > max_chars // 2:
        # Count omitted tables
        remaining = text[last_end + 2:]
        omitted_count = len(re.findall(r"CREATE\s+TABLE\s+", remaining, re.IGNORECASE))
        omitted_note = (
            f"\n\n-- NOTE: Schema truncated at {max_chars} chars. "
            f"{omitted_count} table(s) omitted. "
            f"Only the tables referenced by the SQL and shown above are relevant."
        )
        return truncated[:last_end + 2] + omitted_note

    # Fallback: no good boundary found
    return truncated + "\n\n-- NOTE: Schema truncated. Verify against the execution result below."



def _build_data_preview(data: list, columns: list) -> str:
    """Format a compact, informative data preview."""
    if not data:
        return "(empty result)"

    cols_shown = columns[:12]
    header = ", ".join(str(c) for c in cols_shown)
    lines = [header]

    for row in data[:5]:
        vals = []
        for v in row[:len(cols_shown)]:
            s = str(v)
            if len(s) > _PREVIEW_VALUE_MAX:
                s = s[:_PREVIEW_VALUE_MAX] + "…"
            vals.append(s)
        lines.append(", ".join(vals))

    if len(columns) > 12:
        lines.append(f"-- ({len(columns) - 12} more columns not shown)")
    return "\n".join(lines)


# ── Node ───────────────────────────────────────────────────────────────────────

def semantic_check_node(state: AgentState) -> dict:
    """LLM binary semantic check. Returns semantic_pass / semantic_feedback.

    Escape hatch: if the same SQL (normalized) has been rejected 2 consecutive
    times, skip the check and pass — prevents the Refiner→Generator→SemCheck
    loop from wasting LLM calls on unfixable false negatives.
    """
    question = state.get("question", "")
    sql = state.get("sql", "")
    schema_text = state.get("schema_text", "")
    exec_result = state.get("exec_result") or {}

    if not sql or not question:
        return {"semantic_pass": True, "semantic_feedback": ""}

    # Escape hatch: same SQL rejected ���2 consecutive times → pass
    last_rejected = state.get("_sem_last_rejected_sql", "")
    reject_count = state.get("_sem_reject_count", 0)
    normalized = _normalize_sql(sql)
    if normalized == last_rejected and reject_count >= 2:
        tlog = state.get("tlog")
        if tlog:
            tlog.node_enter("semantic_check", {"sql_len": len(sql), "escape_hatch": True})
            tlog.node_exit("semantic_check", {"passed": True, "escape_hatch": True})
        return {
            "semantic_pass": True,
            "semantic_feedback": f"(escape hatch: same SQL rejected {reject_count} times, passing)",
        }

    tlog = state.get("tlog")
    if tlog:
        tlog.node_enter("semantic_check", {"sql_len": len(sql)})

    row_count = exec_result.get("row_count", 0)
    data = exec_result.get("data", []) or []
    columns = exec_result.get("columns", []) or []

    # B: Filter schema to only tables actually used in the SQL
    used_tables = _extract_sql_tables(sql)
    schema_for_check = _filter_schema_text(schema_text, used_tables, _SCHEMA_MAX_CHARS)

    # C: SQL safety ceiling — boundary-aware truncation (rarely triggered)
    sql_for_check = sql
    if len(sql) > _SQL_MAX_CHARS:
        truncated = sql[:_SQL_MAX_CHARS]
        # Cut at last complete clause keyword
        for kw in [r"\bLIMIT\b", r"\bORDER\s+BY\b", r"\bGROUP\s+BY\b", r"\bHAVING\b",
                    r"\bWHERE\b", r"\bFROM\b", r"\)"]:
            m = None
            for match in re.finditer(kw, truncated, re.IGNORECASE):
                m = match
            if m and m.end() > _SQL_MAX_CHARS // 2:
                sql_for_check = sql[:m.end()] + "\n-- (SQL truncated)"
                break
        else:
            sql_for_check = truncated + "\n-- (SQL truncated)"

    preview_rows = min(len(data), 5)
    preview = _build_data_preview(data, columns)

    prompt = _SEMANTIC_CHECK_PROMPT.format(
        schema_text=schema_for_check,
        question=question,
        sql=sql_for_check,
        row_count=row_count,
        columns=", ".join(str(c) for c in columns[:12]),
        preview_rows=preview_rows,
        preview=preview,
    )

    from langchain_core.messages import SystemMessage, HumanMessage
    from src.infrastructure.llm_factory import get_llm

    chat = get_llm(temperature=0, max_tokens=80, request_timeout=12, max_retries=0)

    tu = {}
    try:
        _t0 = time.time()
        response = chat.invoke([
            SystemMessage(content=SEMANTIC_CHECK_SYSTEM_PROMPT),
            HumanMessage(content=prompt),
        ])
        _duration = round(time.time() - _t0, 3)
        raw = response.content.strip()
        tu = response.response_metadata.get("token_usage", {}) if hasattr(response, "response_metadata") else {}
        if tlog:
            tlog.llm_call(Config.LLM_CHAT_MODEL, tu, _duration)
    except Exception as e:
        tlog = state.get("tlog")
        if tlog:
            tlog.llm_error(Config.LLM_CHAT_MODEL, type(e).__name__, str(e)[:300])
            tlog.node_exit("semantic_check", {"error": str(e)[:120]})
        return {"semantic_pass": True, "semantic_feedback": f"(check skipped: {e})"}

    raw_upper = raw.upper()
    if raw_upper.startswith("YES"):
        passed = True
        reason = ""
    elif raw_upper.startswith("NO"):
        passed = False
        reason = raw[2:].strip().lstrip(":").strip()
    else:
        passed = False
        reason = raw[:200]

    if tlog:
        tlog.semantic_verdict(passed, reason)
        tlog.node_exit("semantic_check", {"passed": passed})

    token_usage = dict(state.get("token_usage", {"prompt": 0, "completion": 0, "total": 0}))
    for k in token_usage:
        token_usage[k] += tu.get(k, 0)

    if not passed:
        new_count = reject_count + 1 if normalized == last_rejected else 1
    else:
        new_count = 0

    result: dict = {
        "semantic_pass": passed,
        "semantic_feedback": reason,
        "token_usage": token_usage,
        "_sem_reject_count": new_count,
        "_sem_last_rejected_sql": normalized if not passed else "",
    }

    if not passed:
        result["last_error"] = f"Semantic check failed: {reason}"
        result["last_sql"] = sql

    return result
