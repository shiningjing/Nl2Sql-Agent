"""Integration smoke tests for W5: trace + router v2 + AST + semantic check + timeout.

These tests exercise the full_graph pipeline end-to-end.
Some tests make LLM calls and require API access.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_full_graph_compiles():
    """Graph compiles with semantic_check node."""
    from src.agent.graphs.full_graph import create_full_graph
    graph = create_full_graph()
    nodes = graph.get_graph().nodes
    assert "schema_retriever" in nodes
    assert "router" in nodes
    assert "generator" in nodes
    assert "guard" in nodes
    assert "voter" in nodes
    assert "semantic_check" in nodes
    assert "refiner" in nodes


def test_trace_id_generated():
    """schema_retriever creates a trace_id and tlog instance."""
    from src.agent.nodes.schema_retriever import schema_retriever_node
    state = {"question": "查询所有用户", "rag_schema": False, "rag_domain": False}
    result = schema_retriever_node(state)
    assert "trace_id" in result
    assert len(result["trace_id"]) == 12
    assert "tlog" in result


def test_router_v2_returns_score_details():
    """Router v2 attaches score/metadata to state."""
    from src.agent.nodes.router import router_node
    result = router_node({"question": "查询所有用户"})
    assert result["complexity"] == "simple"
    assert "router_score" in result
    assert "router_score_detail" in result
    assert "router_method" in result


def test_guard_integrates_ast():
    """Guard node runs AST check alongside regex checks."""
    from src.agent.nodes.guard import guard_node
    result = guard_node({
        "sql": "SELECT * FROM customers WHERE id = 1",
        "schema_text": "CREATE TABLE customers(id int, name text);",
    })
    assert "guard_pass" in result
    assert "ast_pass" in result
    assert "ast_issues" in result


def test_semantic_check_handles_edge_cases():
    """SemanticCheck handles empty/edge inputs without errors."""
    from src.agent.nodes.semantic_check import semantic_check_node
    # Empty question + sql → early return
    r1 = semantic_check_node({"question": "", "sql": "", "exec_result": {}})
    assert r1["semantic_pass"] is True
    # Only one of question/sql present
    r2 = semantic_check_node({"question": "test", "sql": "", "exec_result": {}})
    assert r2["semantic_pass"] is True


def test_executor_timeout_available():
    """run_sql supports timeout parameter."""
    from src.agent.nodes.executor import run_sql, EXEC_TIMEOUT_S
    assert EXEC_TIMEOUT_S == 10
    r = run_sql("SELECT 1", timeout_s=5)
    assert r["success"] is True
    assert r.get("_elapsed_ms", 0) > 0


if __name__ == "__main__":
    test_full_graph_compiles()
    test_trace_id_generated()
    test_router_v2_returns_score_details()
    test_guard_integrates_ast()
    test_semantic_check_handles_edge_cases()
    test_executor_timeout_available()
    print("All W5 integration tests passed!")
