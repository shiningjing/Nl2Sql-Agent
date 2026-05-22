"""Reviewer Agent — static SQL review before execution."""
import json
import re
from langchain_core.messages import SystemMessage, HumanMessage
from .config import Config
from src.infrastructure.llm_factory import get_llm

REVIEWER_PROMPT = """You are a SQL reviewer. Your job is to find errors in a generated SQL query. You NEVER generate SQL yourself — you only review.

## Review checklist
For each of the following categories, check the SQL and report any issues:

1. **Schema alignment**: Does every table name and column name exist in the SCHEMA below? Flag any identifier that is not in the schema (hallucination).
2. **Syntax completeness**: Does every JOIN have an ON condition? Does every GROUP BY include non-aggregated columns? Do subqueries have aliases?
3. **Business rules**: Does the SQL logic match the RETRIEVED NOTES (if provided)? For example, if notes say "valid orders exclude cancelled/refunded", does the SQL filter accordingly?
4. **Dialect compliance**: Are the functions and syntax valid for {dialect}?
5. **Safety**: Does the SQL contain INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, PRAGMA? If so, flag immediately.

## Output format (strict JSON only, no markdown, no extra text)
{
  "valid": true,
  "issues": [],
  "critical_count": 0,
  "suggested_fix": ""
}

or if issues found:

{
  "valid": false,
  "issues": [
    {"type": "hallucination|syntax|business_rule|dialect|safety", "detail": "specific description of the issue"}
  ],
  "critical_count": 2,
  "suggested_fix": "Brief suggestion for how to fix the SQL. Be specific — mention exact column names."
}

## Rules
- If any hallucination is found (column/table not in schema), valid MUST be false.
- Be concise in suggested_fix — one or two sentences.
- Output ONLY the JSON. No markdown code fences, no explanations."""


def get_review_chat():
    return get_llm(temperature=0, request_timeout=45, max_retries=0)


def extract_json(response: str) -> dict:
    """Defensive JSON extraction with fallback."""
    # Layer 1: ```json ... ``` block
    match = re.search(r"```json\s*(.*?)```", response, re.DOTALL | re.IGNORECASE)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass

    # Layer 2: bare { ... } object
    match = re.search(r"\{.*\}", response, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass

    # Layer 3: fail open — treat as valid to avoid blocking
    return {"valid": True, "issues": [], "critical_count": 0, "suggested_fix": "", "_parse_failed": True}


def _hard_check_hallucinations(sql: str, schema_text: str) -> list[dict]:
    """Programmatic check: extract identifiers from SQL, compare against schema."""
    # Parse valid table/column names from schema DDL
    schema_tables = set(re.findall(r"CREATE TABLE (\w+)", schema_text, re.IGNORECASE))
    schema_cols = set()
    for match in re.finditer(r"CREATE TABLE (\w+)\s*\((.*?)\);", schema_text, re.DOTALL | re.IGNORECASE):
        tname = match.group(1)
        body = match.group(2)
        for col_match in re.finditer(r"^\s*(\w+)", body, re.MULTILINE):
            schema_cols.add(col_match.group(1).lower())
            schema_cols.add(f"{tname}.{col_match.group(1)}".lower())

    sql_clean = re.sub(r"'.*?'", "'...'", sql)  # remove string contents
    sql_clean = re.sub(r"`(\w+)`", r"\1", sql_clean)  # unquote backticks
    sql_upper = sql_clean.upper()
    issues = []

    # Find table references in FROM/JOIN
    table_refs = set()
    for m in re.finditer(r"(?:FROM|JOIN)\s+(\w+)", sql_clean, re.IGNORECASE):
        table_refs.add(m.group(1).lower())

    # Check qualified column references (table.column)
    for m in re.finditer(r"(\w+)\.(\w+)", sql_clean):
        ref = f"{m.group(1)}.{m.group(2)}".lower()
        if m.group(1).lower() not in table_refs and m.group(1).lower() not in schema_tables:
            continue  # alias, skip
        if ref not in schema_cols:
            issues.append({
                "type": "hallucination",
                "detail": f"Column '{ref}' not found in schema.",
            })

    # Check bare column names (unqualified), excluding those used in qualified refs
    sql_keywords = {"select","from","where","and","or","not","in","on","as","by",
                    "group","order","having","limit","join","left","right","inner",
                    "outer","cross","full","union","all","distinct","case","when",
                    "then","else","end","between","like","is","null","true","false",
                    "count","sum","avg","min","max","date","strftime","cast","coalesce",
                    "exists","asc","desc","set","into","values","create","drop","alter","table","index","view","database"}
    known_cols = {c for c in schema_cols if "." not in c}

    # Collect qualified column names to exclude their bare parts
    qualified_parts = set()
    for m in re.finditer(r"(\w+)\.(\w+)", sql_clean):
        qualified_parts.add(m.group(2).lower())

    bare_cols = set(re.findall(r"\b([a-zA-Z_]\w*)", sql_upper))
    for col in bare_cols:
        cl = col.lower()
        if cl in sql_keywords or cl.isdigit() or len(cl) <= 1:
            continue
        if cl in known_cols or cl in table_refs or cl in schema_tables:
            continue
        if cl in qualified_parts:
            continue  # Used as table.column, skip bare check
        issues.append({
            "type": "hallucination",
            "detail": f"Identifier '{col}' not found in schema — not a recognized table or column name.",
        })
        break

    return issues


def review(sql: str, schema_text: str, notes_text: str = "", dialect: str = "sqlite") -> dict:
    """Review a SQL query. Returns {valid, issues, critical_count, suggested_fix}.

    Hard check runs first (code-level hallucination detection).
    LLM review supplements with syntax/business/dialect checks.
    """
    # Hard check: programmatic hallucination detection
    hard_issues = _hard_check_hallucinations(sql, schema_text)

    # LLM review: syntax, business rules, dialect, safety
    chat = get_review_chat()

    parts = [f"## SCHEMA\n{schema_text}"]
    if notes_text:
        parts.append(f"## RETRIEVED NOTES\n{notes_text}")
    if hard_issues:
        parts.append(f"## PRE-DETECTED ISSUES (hallucinations confirmed by code)\n{json.dumps(hard_issues, ensure_ascii=False)}")
    parts.append(f"## SQL TO REVIEW\n```sql\n{sql}\n```")

    user_message = "\n\n".join(parts)

    response = chat.invoke([
        SystemMessage(content=REVIEWER_PROMPT.format(dialect=dialect)),
        HumanMessage(content=user_message),
    ])

    result = extract_json(response.content)

    # Extract token usage from LLM response
    tu = response.response_metadata.get("token_usage", {}) if hasattr(response, "response_metadata") else {}
    token_usage = {
        "prompt": tu.get("prompt_tokens", 0),
        "completion": tu.get("completion_tokens", 0),
        "total": tu.get("total_tokens", 0),
    }

    # Deduplicate: extract identifiers from hard_issues, skip LLM issues that repeat them
    seen_ids = set()
    for issue in hard_issues:
        match = re.search(r"'([^']+)'", issue.get("detail", ""))
        if match:
            seen_ids.add(match.group(1).lower())

    llm_issues = result.get("issues", [])
    deduped_llm = []
    for issue in llm_issues:
        match = re.search(r"'([^']+)'", issue.get("detail", ""))
        if match and match.group(1).lower() in seen_ids:
            continue  # Already flagged by hard check, skip LLM duplicate
        deduped_llm.append(issue)

    # Merge hard issues with deduplicated LLM issues
    all_issues = hard_issues + deduped_llm
    critical = len([i for i in all_issues if i.get("type") == "hallucination"])
    result["issues"] = all_issues
    result["critical_count"] = result.get("critical_count", 0) + critical
    if hard_issues:
        result["valid"] = False
    result["token_usage"] = token_usage

    return result
