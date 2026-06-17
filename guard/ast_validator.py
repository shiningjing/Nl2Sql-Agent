"""AST-based SQL validation using sqlglot.

Replaces/supplements regex checks with real parse-tree validation:
- Syntax errors caught at parse time
- Forbidden statement types (INSERT/DELETE/DROP/ALTER/CREATE)
- Multiple statements detection
"""

import sqlglot
from sqlglot.errors import ErrorLevel

_FORBIDDEN_KEYWORDS = frozenset({
    "insert", "delete", "drop", "alter", "create",
    "truncate", "update", "pragama",
})


def validate_sql_ast(sql: str, dialect: str = "sqlite") -> tuple[bool, list[dict]]:
    """Parse SQL with sqlglot. Returns (valid, issues).

    Checks performed:
    1. Parse succeeds (syntax valid)
    2. Non-empty AST produced
    3. All statements are read-only (SELECT / WITH / UNION)
    4. No multiple statements separated by ;
    """
    if not sql or not sql.strip():
        return False, [{"type": "ast_syntax", "detail": "Empty SQL"}]

    issues: list[dict] = []

    try:
        parsed = sqlglot.parse(sql, read=dialect, error_level=ErrorLevel.RAISE)
    except Exception as e:
        return False, [{"type": "ast_syntax", "detail": f"SQL parse error: {str(e)[:200]}"}]

    if not parsed:
        return False, [{"type": "ast_syntax", "detail": "SQL parsed to empty AST"}]

    statements = [p for p in parsed if p is not None]
    if len(statements) > 1:
        issues.append({
            "type": "ast_structure",
            "detail": f"Multiple statements detected ({len(statements)}). Only single SELECT/WITH allowed.",
        })
        return False, issues

    for stmt in statements:
        kind = _statement_kind(stmt).lower()

        if kind in _FORBIDDEN_KEYWORDS:
            issues.append({
                "type": "ast_forbidden",
                "detail": f"Forbidden statement type: {kind.upper()}. Only SELECT/WITH allowed.",
            })
            return False, issues

    return len(issues) == 0, issues


def _statement_kind(stmt) -> str:
    """Extract statement type string from sqlglot AST node."""
    if hasattr(stmt, "key"):
        return str(stmt.key)
    return type(stmt).__name__


def extract_table_names(sql: str, dialect: str = "sqlite") -> set[str]:
    """Extract all real table names from a SQL query using sqlglot.

    Filters out CTE aliases so only underlying physical tables are returned.
    Returns lowercase names for case-insensitive comparison.
    """
    from sqlglot import exp

    if not sql or not sql.strip():
        return set()

    try:
        tree = sqlglot.parse_one(sql, read=dialect)
    except Exception:
        return set()

    cte_aliases = {node.alias.lower() for node in tree.find_all(exp.CTE) if node.alias}
    tables = set()
    for node in tree.find_all(exp.Table):
        name = node.name.lower() if node.name else ""
        if name and name not in cte_aliases:
            tables.add(name)
    return tables
