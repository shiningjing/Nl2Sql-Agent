"""MCP tool: validate_sql — L1 regex + L3 AST SQL validation.

Usage:
    python tools/mcp/validate_sql_server.py
    # Stdio MCP server, connect via fastmcp Client or MCP inspector.

Input:  {sql, dialect}
Output: {valid, issues[], statement_type, table_references}
"""

import re
from fastmcp import FastMCP

import sqlglot
from sqlglot.errors import ErrorLevel

mcp = FastMCP("validate-sql")

# ── L1: regex-based checks ──────────────────────────────────────────────

_FORBIDDEN_KEYWORDS = frozenset({
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE", "PRAGMA",
})


def _l1_regex_check(sql: str) -> list[dict]:
    """Fast regex-based safety checks. Returns issues found."""
    issues: list[dict] = []

    if not sql or not sql.strip():
        issues.append({"type": "empty_sql", "detail": "SQL is empty or whitespace only."})
        return issues

    sql_upper = sql.upper()

    # Must contain SELECT or WITH
    if "SELECT" not in sql_upper and not sql_upper.startswith("WITH"):
        issues.append({
            "type": "missing_select",
            "detail": "SQL does not contain SELECT or WITH.",
        })

    # Forbidden DML/DDL keywords
    for kw in sorted(_FORBIDDEN_KEYWORDS):
        if kw in sql_upper:
            issues.append({
                "type": "forbidden_keyword",
                "detail": f"Forbidden keyword: {kw}. Only SELECT/WITH allowed.",
            })

    # Multi-statement via semicolons
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    if len(statements) > 1:
        issues.append({
            "type": "multi_statement",
            "detail": f"Multiple statements detected ({len(statements)}). Only single SELECT/WITH allowed.",
        })

    return issues


# ── L3: AST-based checks (sqlglot) ──────────────────────────────────────

_AST_FORBIDDEN = frozenset({
    "insert", "delete", "drop", "alter", "create",
    "truncate", "update", "pragma",
})


def _l3_ast_check(sql: str, dialect: str) -> tuple[list[dict], str, list[str]]:
    """sqlglot AST validation. Returns (issues, statement_type, table_references)."""
    issues: list[dict] = []
    statement_type = ""
    table_references: list[str] = []

    if not sql or not sql.strip():
        return issues, statement_type, table_references

    # Parse with sqlglot
    try:
        parsed = sqlglot.parse(sql, read=dialect, error_level=ErrorLevel.RAISE)
    except Exception as e:
        issues.append({
            "type": "ast_syntax",
            "detail": f"SQL parse error: {str(e)[:200]}",
        })
        return issues, statement_type, table_references

    if not parsed:
        issues.append({
            "type": "ast_syntax",
            "detail": "SQL parsed to empty AST.",
        })
        return issues, statement_type, table_references

    statements = [p for p in parsed if p is not None]
    if len(statements) > 1:
        issues.append({
            "type": "ast_structure",
            "detail": f"Multiple statements detected ({len(statements)}). Only single SELECT/WITH allowed.",
        })
        return issues, statement_type, table_references

    # Inspect the single statement
    stmt = statements[0]
    statement_type = _statement_kind(stmt).upper()

    if statement_type.lower() in _AST_FORBIDDEN:
        issues.append({
            "type": "ast_forbidden",
            "detail": f"Forbidden statement type: {statement_type}. Only SELECT/WITH allowed.",
        })

    # Extract table references (filter out CTE aliases)
    try:
        from sqlglot import exp
        tree = sqlglot.parse_one(sql, read=dialect)
        cte_aliases = {node.alias.lower() for node in tree.find_all(exp.CTE) if node.alias}
        tables = set()
        for node in tree.find_all(exp.Table):
            name = node.name.lower() if node.name else ""
            if name and name not in cte_aliases:
                tables.add(name)
        table_references = sorted(tables)
    except Exception:
        pass

    return issues, statement_type, table_references


def _statement_kind(stmt) -> str:
    if hasattr(stmt, "key"):
        return str(stmt.key)
    return type(stmt).__name__


# ── Combined tool ────────────────────────────────────────────────────────

@mcp.tool
def validate_sql(sql: str, dialect: str = "sqlite") -> dict:
    """Validate a SQL string for safety and syntax.

    Performs L1 regex checks (forbidden keywords, multi-statement) then
    L3 AST validation via sqlglot. Returns structured issues and metadata.

    Args:
        sql: The SQL string to validate.
        dialect: SQL dialect for parsing (sqlite, postgres, mysql, etc.).
    """
    issues: list[dict] = []

    # L1: regex checks first (fast, no dependencies)
    issues.extend(_l1_regex_check(sql))

    # L3: AST checks (more accurate, also extracts metadata)
    ast_issues, statement_type, table_references = _l3_ast_check(sql, dialect)
    issues.extend(ast_issues)

    # If L1 already found critical issues, AST may still provide statement_type
    # and table_references — include them if available.
    valid = len(issues) == 0

    return {
        "valid": valid,
        "issues": issues,
        "statement_type": statement_type,
        "table_references": table_references,
    }


# ── Entrypoint ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    mcp.run()
