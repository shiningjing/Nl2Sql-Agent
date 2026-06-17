"""Tests for SemanticCheck node."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_semantic_check_empty_state():
    """semantic_check handles empty sql/exec_result gracefully (no LLM call)."""
    from agent.nodes.semantic_check import semantic_check_node
    result = semantic_check_node({"question": "", "sql": "", "exec_result": {}})
    assert result.get("semantic_pass") is True


def test_semantic_check_no_sql():
    from agent.nodes.semantic_check import semantic_check_node
    result = semantic_check_node({"question": "test", "sql": "", "exec_result": {}})
    assert result.get("semantic_pass") is True


def test_semantic_check_no_question():
    from agent.nodes.semantic_check import semantic_check_node
    result = semantic_check_node({"question": "", "sql": "SELECT 1", "exec_result": {}})
    assert result.get("semantic_pass") is True


def test_semantic_check_returns_fields():
    """semantic_check returns all required keys."""
    from agent.nodes.semantic_check import semantic_check_node
    result = semantic_check_node({
        "question": "查询所有用户",
        "sql": "SELECT * FROM customers",
        "schema_text": "CREATE TABLE customers(id int, name text);",
        "exec_result": {
            "success": True, "row_count": 1,
            "data": [(1, "Alice")], "columns": ["id", "name"],
        },
    })
    assert "semantic_pass" in result
    assert "semantic_feedback" in result
    # When passed: no last_error
    # When failed: last_error should be set


def test_full_graph_has_semantic_check():
    """full_graph includes semantic_check node."""
    from agent.graphs.full_graph import create_full_graph
    from agent.state import AgentState
    graph = create_full_graph()
    nodes = graph.get_graph().nodes
    assert "semantic_check" in nodes


if __name__ == "__main__":
    test_semantic_check_empty_state()
    test_semantic_check_no_sql()
    test_semantic_check_no_question()
    test_semantic_check_returns_fields()
    test_full_graph_has_semantic_check()
    print("All SemanticCheck tests passed!")
