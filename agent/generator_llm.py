import re
from langchain_core.messages import SystemMessage, HumanMessage
from storage.config import Config

from agent.prompts import GENERATOR_SYSTEM_PROMPT as SYSTEM_PROMPT
from agent.llm_factory import get_llm


def get_chat_model():
    return get_llm(temperature=0, request_timeout=45, max_retries=0)


def extract_sql(response: str) -> str:
    """Defensive SQL extraction with multi-layer fallback."""
    # Layer 1: extract ```sql ... ``` block
    match = re.search(r"```sql\s*(.*?)```", response, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()

    # Layer 2: find SELECT/WITH statement
    match = re.search(r"(?:WITH\s+.*?SELECT|SELECT)\s+.*?;", response, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(0).strip()

    # Layer 3: return raw response as fallback (will be caught by validation)
    return response.strip()


def _extract_token_usage(response) -> dict:
    tu = response.response_metadata.get("token_usage", {}) if hasattr(response, "response_metadata") else {}
    return {
        "prompt": tu.get("prompt_tokens", 0),
        "completion": tu.get("completion_tokens", 0),
        "total": tu.get("total_tokens", 0),
    }


def _format_sub_questions(sub_questions: list[dict]) -> str:
    lines = []
    for s in sub_questions:
        deps = s.get("depends_on", [])
        dep_str = f" (depends on steps: {deps})" if deps else ""
        lines.append(f"Step {s['id']}: {s['sub_q']}{dep_str}")
    return "\n".join(lines)


def get_dialect_from_url(database_url: str) -> str:
    """Derive SQL dialect from database URL scheme."""
    for prefix, dialect in [
        ("postgresql", "postgresql"),
        ("mysql", "mysql"),
        ("sqlite", "sqlite"),
    ]:
        if database_url.startswith(prefix):
            return dialect
    return "sqlite"


def _load_dialect_rules() -> dict[str, list[str]]:
    """Load dialect rules from JSON file, with inline defaults as fallback."""
    import json
    import os

    # Try project-relative path first, then package-relative
    candidates = [
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "dialect_rules.json"),
        os.path.join(os.getcwd(), "data", "dialect_rules.json"),
    ]
    for path in candidates:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)

    # Inline fallback (kept in sync with data/dialect_rules.json)
    return {
        "sqlite": [
            "Use GROUP_CONCAT(expr, separator) for string aggregation.",
            "Use double-quoted identifiers for reserved words (e.g., \"column_name\").",
            "Use strftime() for date formatting (e.g., strftime('%Y-%m', date_col)).",
            "Use || for string concatenation (e.g., first_name || ' ' || last_name).",
            "Boolean values: use 0 and 1 (no TRUE/FALSE keywords).",
            "INTEGER PRIMARY KEY is auto-incrementing by default.",
            "Use CAST(expr AS type) for type conversion (e.g., CAST(price AS INTEGER)).",
            "No window function support before version 3.25. Avoid ROW_NUMBER(), RANK(), etc. unless certain.",
            "Use ROUND(value, decimals) for rounding numbers.",
            "No RIGHT JOIN or FULL OUTER JOIN. Use LEFT JOIN instead and reorder tables if needed.",
        ],
        "postgresql": [
            "Use STRING_AGG(expr, delimiter ORDER BY ...) for string aggregation.",
            "Use ::type for type casting (e.g., value::integer, value::text, value::numeric).",
            "Use ILIKE for case-insensitive pattern matching (e.g., name ILIKE '%abc%').",
            "Use DISTINCT ON (column) to keep the first row per group.",
            "Use double-quoted identifiers for reserved words or mixed case (e.g., \"UserName\").",
            "Use || for string concatenation (e.g., first_name || ' ' || last_name).",
            "Use DATE_TRUNC('unit', timestamp) for date truncation (e.g., DATE_TRUNC('month', created_at)).",
            "Use LIMIT n OFFSET m for pagination (not LIMIT m, n).",
            "Use BOOL_AND(expr) / BOOL_OR(expr) for boolean aggregation.",
            "Use FILTER (WHERE ...) clause on aggregates for conditional aggregation (e.g., COUNT(*) FILTER (WHERE status = 'active')).",
        ],
        "mysql": [
            "Use GROUP_CONCAT(expr ORDER BY ... SEPARATOR '...') for string aggregation.",
            "Use backtick-quoted identifiers for reserved words (e.g., `order`, `status`).",
            "Use DATE_FORMAT(date_col, '%Y-%m') for date formatting.",
            "Use LIMIT offset, count for pagination (e.g., LIMIT 10, 20 means skip 10, return 20).",
            "Use AUTO_INCREMENT for auto-incrementing primary keys.",
            "Use CONCAT(str1, str2, ...) for string concatenation (|| is not supported).",
            "Use IFNULL(expr, default) or COALESCE(expr, default) for NULL handling.",
            "Use YEAR(date_col), MONTH(date_col), DAY(date_col) to extract date parts.",
            "Use REGEXP for regular expression matching (e.g., name REGEXP '^[A-Z]').",
            "No FULL OUTER JOIN. Emulate with LEFT JOIN UNION RIGHT JOIN if needed.",
        ],
    }


def _get_dialect_rules(dialect: str) -> str:
    """Return dialect-specific SQL syntax hints for the LLM prompt."""
    d = dialect.lower()
    all_rules = _load_dialect_rules()
    rules = all_rules.get(d, all_rules.get("sqlite", []))
    return "\n".join(f"- {r}" for r in rules)


def generate_sql(
    schema_text: str,
    user_question: str,
    rag_context: str = "",
    sample_rows_text: str = "",
    last_error: str = "",
    last_sql: str = "",
    sub_questions: list[dict] | None = None,
    dialect: str | None = None,
) -> tuple[str, str, dict]:
    """
    Generate SQL via LLM. Returns (sql, raw_response, token_usage).
    """
    chat = get_chat_model()
    _dialect = (dialect or Config.SQL_DIALECT).upper()
    system = SYSTEM_PROMPT.format(
        dialect=_dialect,
        dialect_rules=_get_dialect_rules(_dialect),
    )

    parts = [f"## SCHEMA\n{schema_text}"]
    if sample_rows_text:
        parts.append(f"## SAMPLE ROWS\n{sample_rows_text}")
    if rag_context:
        parts.append(f"## RETRIEVED NOTES\n{rag_context}")
    if sub_questions:
        parts.append(f"## SUB_QUESTIONS (use CTE/WITH to implement each step)\n{_format_sub_questions(sub_questions)}")
    parts.append(f"## USER QUESTION\n{user_question}")
    if last_error and last_sql:
        parts.append(f"## LAST_ERROR\n{last_error}")
        parts.append(f"## LAST_SQL\n{last_sql}")

    user_message = "\n\n".join(parts)

    response = chat.invoke([
        SystemMessage(content=system),
        HumanMessage(content=user_message),
    ])

    raw = response.content
    token_usage = _extract_token_usage(response)
    sql = extract_sql(raw)
    valid, reason = validate_sql(sql)

    if not valid:
        # Treat validation failure like a format error
        raw = f"Validation failed: {reason}\n\nOriginal response:\n{raw}"
        sql = ""

    return sql, raw, token_usage
