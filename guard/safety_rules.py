"""Unified SQL safety rules — single entry point for all validation.

Two functions:
  check_safety(sql, dialect)           → pure syntax check, no schema dependency
  check_hallucinations(sql, schema_info) → schema cross-check, AST-based
"""

import re

import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel

# ── Constants ────────────────────────────────────────────────────────────

_FORBIDDEN_KEYWORDS = frozenset({
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE", "PRAGMA",
})

_AST_FORBIDDEN = frozenset({
    "insert", "delete", "drop", "alter", "create", "truncate", "update", "pragma",
})


# ── check_safety: L1 regex + L3 AST ──────────────────────────────────────

def check_safety(sql: str, dialect: str = "sqlite") -> dict:
    """Validate SQL for syntax safety. No schema dependency.

    Returns:
        {"valid": bool, "issues": [{"type": str, "detail": str}, ...],
         "statement_type": str, "table_references": [str, ...]}
    """
    issues: list[dict] = []

    # L1: empty check
    if not sql or not sql.strip():
        return {
            "valid": False,
            "issues": [{"type": "empty_sql", "detail": "SQL is empty or whitespace only."}],
            "statement_type": "",
            "table_references": [],
        }

    sql_upper = sql.upper()

    # L1: require SELECT or WITH
    if "SELECT" not in sql_upper and not sql_upper.startswith("WITH"):
        issues.append({
            "type": "missing_select",
            "detail": "SQL does not contain SELECT or WITH.",
        })

    # L1: forbidden keywords
    for kw in sorted(_FORBIDDEN_KEYWORDS):
        if kw in sql_upper:
            issues.append({
                "type": "forbidden_keyword",
                "detail": f"Forbidden keyword: {kw}. Only SELECT/WITH allowed.",
            })

    # L1: multi-statement via semicolons
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    if len(statements) > 1:
        issues.append({
            "type": "multi_statement",
            "detail": f"Multiple statements detected ({len(statements)}). Only single SELECT/WITH allowed.",
        })

    # L3: AST checks (sqlglot)
    ast_issues, stmt_type, tables = _ast_check(sql, dialect)
    issues.extend(ast_issues)

    return {
        "valid": len(issues) == 0,
        "issues": issues,
        "statement_type": stmt_type,
        "table_references": tables,
    }


def _ast_check(sql: str, dialect: str) -> tuple[list[dict], str, list[str]]:
    """sqlglot AST validation and metadata extraction."""
    issues: list[dict] = []
    statement_type = ""
    table_references: list[str] = []

    try:
        parsed = sqlglot.parse(sql, read=dialect, error_level=ErrorLevel.RAISE)
    except Exception as e:
        issues.append({
            "type": "ast_syntax",
            "detail": f"SQL parse error: {str(e)[:200]}",
        })
        return issues, statement_type, table_references

    if not parsed:
        issues.append({"type": "ast_syntax", "detail": "SQL parsed to empty AST."})
        return issues, statement_type, table_references

    stmts = [p for p in parsed if p is not None]
    if len(stmts) > 1:
        issues.append({
            "type": "ast_structure",
            "detail": f"Multiple statements detected ({len(stmts)}). Only single SELECT/WITH allowed.",
        })
        return issues, statement_type, table_references

    stmt = stmts[0]
    statement_type = _statement_kind(stmt).upper()

    if statement_type.lower() in _AST_FORBIDDEN:
        issues.append({
            "type": "ast_forbidden",
            "detail": f"Forbidden statement type: {statement_type}. Only SELECT/WITH allowed.",
        })

    # Extract table references (filter CTE aliases)
    try:
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


# ── check_hallucinations: schema cross-check ─────────────────────────────

_SQL_KW = frozenset({
    "select", "from", "where", "and", "or", "not", "in", "on", "as", "by",
    "group", "order", "having", "limit", "join", "left", "right", "inner",
    "outer", "cross", "full", "union", "all", "distinct", "case", "when",
    "then", "else", "end", "between", "like", "is", "null", "true", "false",
    "count", "sum", "avg", "min", "max", "date", "strftime", "cast", "coalesce",
    "exists", "asc", "desc", "set", "into", "values", "create", "drop",
    "alter", "table", "index", "view", "database", "with", "over", "partition",
    "window", "row", "number", "rank", "dense", "first", "last",
    "round", "abs", "random", "length", "substr", "substring", "replace",
    "trim", "ltrim", "rtrim", "upper", "lower", "typeof", "ifnull",
    "printf", "hex", "unicode", "char", "instr", "total", "group_concat",
    "zeroblob", "changes", "last_insert_rowid", "sqlite_version",
    "primary", "key", "foreign", "references", "default", "check", "unique",
    "constraint", "cascade", "integer", "text", "real", "blob", "numeric",
    "varchar", "datetime", "timestamp",
})


def check_hallucinations(sql: str, schema_info: dict) -> dict:
    """Cross-check SQL identifiers against schema. Returns {valid, issues[]}.

    schema_info: {table_name: {"columns": [{"name": "col1"}, ...]}}
    """
    issues: list[dict] = []

    schema_tables: set[str] = set()
    schema_cols: set[str] = set()
    qualified_cols: set[str] = set()

    for t_name, t_info in schema_info.items():
        schema_tables.add(t_name.lower())
        for c in t_info.get("columns", []):
            cn = c["name"].lower()
            schema_cols.add(cn)
            qualified_cols.add(f"{t_name.lower()}.{cn}")

    try:
        tree = sqlglot.parse_one(sql)
    except Exception:
        return {"valid": False, "issues": [{"type": "hallucination", "detail": "SQL parse error, cannot check schema references."}]}

    # Collect CTE names and table aliases (safe to ignore)
    cte_names = {node.alias.lower() for node in tree.find_all(exp.CTE) if node.alias}
    table_aliases: set[str] = set()
    for node in tree.find_all(exp.Table):
        if node.alias:
            table_aliases.add(node.alias.lower())

    # Extract table references from FROM/JOIN (real tables, not CTE aliases)
    table_refs = set()
    for node in tree.find_all(exp.Table):
        name = node.name.lower() if node.name else ""
        if name and name not in cte_names:
            table_refs.add(name)

    # Check table references exist in schema
    for t in table_refs:
        if t not in schema_tables:
            issues.append({
                "type": "hallucination",
                "detail": f"Table '{t}' not found in schema.",
            })

    # Check qualified column references (table.col)
    for ref_node in tree.find_all(exp.Column):
        if ref_node.table:
            ref = f"{ref_node.table.lower()}.{ref_node.name.lower()}"
            if ref_node.table.lower() in cte_names or ref_node.table.lower() in table_aliases:
                continue
            if ref not in qualified_cols:
                issues.append({
                    "type": "hallucination",
                    "detail": f"Column '{ref}' not found in schema.",
                })

    # Check bare column references (no table prefix)
    for col_node in tree.find_all(exp.Column):
        if col_node.table:
            continue  # qualified refs handled above
        col_name = col_node.name.lower()
        if col_name in _SQL_KW or col_name.isdigit() or len(col_name) <= 1:
            continue
        if col_name in schema_cols or col_name in cte_names or col_name in table_aliases:
            continue
        if col_name == "*":
            continue
        # Check if this is a SELECT alias (FROM subquery or regular alias)
        # We skip identifiers that might be SELECT aliases
        if _is_select_alias(tree, col_name):
            continue
        issues.append({
            "type": "hallucination",
            "detail": f"Identifier '{col_name}' not in schema.",
        })
        break  # one bare-col issue is enough to signal, avoid noise

    return {"valid": len(issues) == 0, "issues": issues}


def _is_select_alias(tree, name: str) -> bool:
    """Check if `name` is a SELECT alias (safe identifier)."""
    for node in tree.find_all(exp.Alias):
        if node.alias and node.alias.lower() == name:
            return True
    return False
