"""Guard node — hard SQL validation before execution. Zero LLM cost."""
import time
from agent.state import AgentState
from guard.safety_rules import check_safety, check_hallucinations


def _get_sqlglot_dialect(database_url: str | None) -> str:
    """Map database URL to sqlglot dialect name."""
    if not database_url:
        return "sqlite"
    from agent.generator_llm import get_dialect_from_url
    d = get_dialect_from_url(database_url)
    if d == "postgresql":
        return "postgres"
    return d


def _get_cached_schema_for_check(database_url: str | None) -> dict:
    """Extract schema info for hallucination check."""
    if not database_url:
        return {}
    try:
        from retrieval.schema import _get_cached_schema_info
        return _get_cached_schema_info(database_url)
    except Exception:
        return {}


def guard_node(state: AgentState) -> dict:
    """Validate SQL against schema + safety rules. Sets guard_pass / guard_issues / ast_pass / ast_issues."""
    t0 = time.time()
    sql = state.get("sql", "")

    tlog = state.get("tlog")
    if tlog:
        tlog.node_enter("guard", {"sql_len": len(sql)})

    # Unified safety check (L1 regex + L3 AST)
    dialect = _get_sqlglot_dialect(state.get("database_url"))
    safety_result = check_safety(sql, dialect)
    issues: list[dict] = list(safety_result["issues"])
    ast_pass = all(i["type"].startswith("ast_") for i in issues) if issues else True

    # Hallucination check (schema cross-check)
    schema_info = _get_cached_schema_for_check(state.get("database_url"))
    if schema_info:
        hallu_result = check_hallucinations(sql, schema_info)
        issues.extend(hallu_result["issues"])

    passed = len(issues) == 0

    if tlog:
        tlog.guard_result(passed, issues)
        tlog.node_exit("guard", {"passed": passed, "issue_count": len(issues)})

    node_latency = dict(state.get("node_latency", {}))
    node_latency["guard"] = round(time.time() - t0, 3)

    return {
        "guard_pass": passed,
        "guard_issues": issues,
        "ast_pass": ast_pass,
        "ast_issues": issues,
        "node_latency": node_latency,
    }
