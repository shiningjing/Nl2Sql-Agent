"""Guard node — hard SQL validation before execution. Zero LLM cost."""
import re
from agent.state import AgentState
from guard.ast_validator import validate_sql_ast


def _validate_sql(sql: str) -> tuple[bool, str]:
    """Lightweight safety/structural check."""
    sql_upper = sql.upper()
    if "SELECT" not in sql_upper:
        return False, "SQL does not contain SELECT"
    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "PRAGMA"]
    for kw in forbidden:
        if kw in sql_upper:
            return False, f"Forbidden keyword: {kw}"
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    if len(statements) > 1:
        return False, "Multiple statements detected"
    return True, ""


def _get_schema_info(database_url: str | None) -> tuple[set[str], set[str]]:
    """Extract table and column names from cached schema metadata (zero PRAGMA)."""
    if not database_url:
        return set(), set()
    try:
        from retrieval.schema import _get_cached_schema_info
        info = _get_cached_schema_info(database_url)
        tables = set()
        cols: set[str] = set()
        for t_lower, t_info in info.items():
            tables.add(t_lower)
            for c in t_info["columns"]:
                col_name = c["name"].lower()
                cols.add(col_name)
                cols.add(f"{t_lower}.{col_name}")
        return tables, cols
    except Exception:
        return set(), set()


def _check_hallucinations(sql: str, schema_text: str, database_url: str | None = None) -> list[dict]:
    """Extract identifiers from SQL and cross-check against schema via SQLAlchemy inspect."""
    schema_tables, schema_cols = _get_schema_info(database_url)

    sql_clean = re.sub(r"'.*?'", "'...'", sql)
    sql_clean = re.sub(r"`(\w+)`", r"\1", sql_clean)
    sql_clean = re.sub(r'"[^"]*"', '', sql_clean)          # strip double-quoted identifiers (SQLite/PG)
    sql_clean = re.sub(r"--[^\n]*", "", sql_clean)          # strip single-line comments
    sql_clean = re.sub(r"/\*.*?\*/", "", sql_clean, flags=re.DOTALL)  # strip multi-line comments
    issues: list[dict] = []

    # Parse CTE names: WITH step1 AS (...), step2 AS (...)
    cte_names: set[str] = set()
    for m in re.finditer(r"WITH\s+(\w+)\s+AS\s*\(", sql_clean, re.IGNORECASE):
        cte_names.add(m.group(1).lower())
    # Also match CTE names after commas: , step2 AS (...)
    for m in re.finditer(r",\s*(\w+)\s+AS\s*\(", sql_clean, re.IGNORECASE):
        cte_names.add(m.group(1).lower())

    # Parse table references and their aliases
    table_refs: set[str] = set()
    aliases: set[str] = set()
    for m in re.finditer(
        r"(?:FROM|JOIN)\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?",
        sql_clean, re.IGNORECASE,
    ):
        table_refs.add(m.group(1).lower())
        if m.group(2):
            aliases.add(m.group(2).lower())

    # Parse SELECT aliases: SELECT name AS customer_name, COUNT(*) AS cnt
    select_aliases: set[str] = set()
    for m in re.finditer(r"(?:AS\s+)(\w+)", sql_clean, re.IGNORECASE):
        select_aliases.add(m.group(1).lower())

    # Qualified ref check: skip CTE-qualified refs (step1.col comes from CTE body, trusted)
    for m in re.finditer(r"(\w+)\.(\w+)", sql_clean):
        prefix = m.group(1).lower()
        ref = f"{prefix}.{m.group(2)}".lower()
        # Skip if prefix is a CTE name, alias, or unknown (not a schema table)
        if prefix in cte_names or prefix in aliases:
            continue
        if prefix not in table_refs and prefix not in schema_tables:
            continue
        if ref not in schema_cols:
            issues.append({
                "type": "hallucination",
                "detail": f"Column '{ref}' not found in schema.",
            })

    _SQL_KW = {
        "select", "from", "where", "and", "or", "not", "in", "on", "as", "by",
        "group", "order", "having", "limit", "join", "left", "right", "inner",
        "outer", "cross", "full", "union", "all", "distinct", "case", "when",
        "then", "else", "end", "between", "like", "is", "null", "true", "false",
        "count", "sum", "avg", "min", "max", "date", "strftime", "cast", "coalesce",
        "exists", "asc", "desc", "set", "into", "values", "create", "drop",
        "alter", "table", "index", "view", "database", "with", "over", "partition",
        "having", "window", "row", "number", "rank", "dense", "first", "last",
        # SQLite math functions
        "round", "abs", "random", "length", "substr", "substring", "replace",
        "trim", "ltrim", "rtrim", "upper", "lower", "typeof", "ifnull",
        "printf", "hex", "unicode", "char", "instr", "total", "group_concat",
        "zeroblob", "changes", "last_insert_rowid", "sqlite_version",
    }
    known_cols = {c for c in schema_cols if "." not in c}
    qualified_parts = set()
    for m in re.finditer(r"(\w+)\.(\w+)", sql_clean):
        qualified_parts.add(m.group(2).lower())

    # All identifiers that are safe: schema tables, table refs, CTE names, aliases
    safe_ids = schema_tables | table_refs | cte_names | aliases | known_cols | select_aliases

    sql_upper = sql_clean.upper()
    bare_cols = set(re.findall(r"\b([a-zA-Z_]\w*)", sql_upper))
    for col in bare_cols:
        cl = col.lower()
        if cl in _SQL_KW or cl.isdigit() or len(cl) <= 1:
            continue
        if cl in safe_ids or cl in qualified_parts:
            continue
        issues.append({
            "type": "hallucination",
            "detail": f"Identifier '{col}' not in schema.",
        })
        break

    return issues


def _get_sqlglot_dialect(database_url: str | None) -> str:
    """Map database URL to sqlglot dialect name."""
    if not database_url:
        return "sqlite"
    from agent.generator_llm import get_dialect_from_url
    d = get_dialect_from_url(database_url)
    # sqlglot uses "postgres" not "postgresql"
    if d == "postgresql":
        return "postgres"
    return d


def guard_node(state: AgentState) -> dict:
    """Validate SQL against schema + safety rules. Sets guard_pass / guard_issues / ast_pass / ast_issues."""
    sql = state.get("sql", "")
    schema_text = state.get("schema_text", "")

    tlog = state.get("tlog")
    if tlog:
        tlog.node_enter("guard", {"sql_len": len(sql)})

    issues: list[dict] = []

    # Layer 1: Regex safety check
    valid, reason = _validate_sql(sql)
    if not valid:
        issues.append({"type": "safety", "detail": reason})

    # Layer 2: Hallucination check
    issues.extend(_check_hallucinations(sql, schema_text, state.get("database_url")))

    # Layer 3: AST structural validation (W5)
    ast_dialect = _get_sqlglot_dialect(state.get("database_url"))
    ast_pass, ast_issues = validate_sql_ast(sql, dialect=ast_dialect)
    issues.extend(ast_issues)

    passed = len(issues) == 0

    tlog = state.get("tlog")
    if tlog:
        tlog.guard_result(passed, issues)
        tlog.node_exit("guard", {"passed": passed, "issue_count": len(issues)})

    return {
        "guard_pass": passed,
        "guard_issues": issues,
        "ast_pass": ast_pass,
        "ast_issues": ast_issues,
    }
