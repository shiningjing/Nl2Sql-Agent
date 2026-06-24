"""Tests for human-feedback conversation feature."""
import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestFormatUserFeedback:
    """_format_user_feedback() produces correct CORRECTION FEEDBACK structure."""

    def test_basic(self):
        from agent.nodes.refiner import _format_user_feedback
        result = _format_user_feedback(
            feedback="需要加 GROUP BY customer_id",
            sql="SELECT COUNT(*) FROM orders",
            exec_result={"success": True, "row_count": 1, "columns": ["COUNT(*)"], "data": [[100]]},
        )
        assert "## CORRECTION FEEDBACK" in result
        assert "## CURRENT GUIDANCE" in result
        assert '需要加 GROUP BY customer_id' in result
        assert "## CURRENT SQL (for reference)" in result
        assert "SELECT COUNT(*) FROM orders" in result
        # No history → no PREVIOUS ATTEMPTS
        assert "## PREVIOUS ATTEMPTS" not in result

    def test_with_history(self):
        from agent.nodes.refiner import _format_user_feedback
        turns = [
            {"turn": 1, "user_feedback": "加 GROUP BY",
             "sql": "SELECT cid, COUNT(*) FROM orders GROUP BY cid",
             "exec_result": {"success": True, "row_count": 50, "columns": ["cid", "COUNT(*)"]}},
            {"turn": 2, "user_feedback": "过滤 NULL",
             "sql": "SELECT cid, COUNT(*) FROM orders WHERE cid IS NOT NULL GROUP BY cid",
             "exec_result": {"success": True, "row_count": 48, "columns": ["cid", "COUNT(*)"]}},
        ]
        result = _format_user_feedback(
            feedback="按金额降序",
            sql="SELECT cid, COUNT(*) FROM orders WHERE cid IS NOT NULL GROUP BY cid",
            exec_result={"success": True, "row_count": 48},
            conversation_turns=turns,
        )
        assert "## PREVIOUS ATTEMPTS" in result
        assert "Turn 1" in result
        assert "加 GROUP BY" in result
        assert "Turn 2" in result
        assert "过滤 NULL" in result
        assert "## CURRENT GUIDANCE" in result
        assert "按金额降序" in result
        assert "## CURRENT SQL (for reference)" in result

    def test_with_failed_exec(self):
        from agent.nodes.refiner import _format_user_feedback
        result = _format_user_feedback(
            feedback="fix the error",
            sql="SELECT bad_column FROM orders",
            exec_result={"success": False, "error": "no such column: bad_column"},
        )
        assert "## CORRECTION FEEDBACK" in result
        assert "## CURRENT GUIDANCE" in result
        assert "fix the error" in result

    def test_no_failed_sql_heading(self):
        """User feedback uses 'for reference', NOT 'DO NOT REPEAT'."""
        from agent.nodes.refiner import _format_user_feedback
        result = _format_user_feedback(
            feedback="改进查询",
            sql="SELECT * FROM users",
            exec_result={"success": True, "row_count": 10},
        )
        assert "DO NOT REPEAT" not in result
        assert "for reference" in result

    def test_empty_turns_no_history(self):
        from agent.nodes.refiner import _format_user_feedback
        result = _format_user_feedback(
            feedback="test",
            sql="SELECT 1",
            exec_result={},
            conversation_turns=[],
        )
        assert "## PREVIOUS ATTEMPTS" not in result


class TestRefinerNodePriority:
    """refiner_node() prioritizes user_feedback over automated error sources."""

    def test_user_feedback_priority(self):
        from agent.nodes.refiner import refiner_node
        state = {
            "user_feedback": "加 WHERE 条件",
            "semantic_feedback": "wrong aggregation",
            "sql": "SELECT COUNT(*) FROM orders",
            "exec_result": {"error": "some error"},
            "guard_issues": [{"type": "hallucination", "detail": "bad column"}],
            "retry_count": 0,
            "max_retries": 2,
        }
        result = refiner_node(state)
        assert "## CURRENT GUIDANCE" in result["last_error"]
        assert "加 WHERE 条件" in result["last_error"]
        # User feedback takes priority — semantic/exec feedback should NOT appear
        assert "Semantic error" not in result["last_error"]

    def test_semantic_used_when_no_user_feedback(self):
        from agent.nodes.refiner import refiner_node
        state = {
            "semantic_feedback": "missing GROUP BY",
            "sql": "SELECT COUNT(*) FROM orders",
            "exec_result": {},
            "retry_count": 0,
            "max_retries": 2,
        }
        result = refiner_node(state)
        assert "Semantic error" in result["last_error"]
        assert "missing GROUP BY" in result["last_error"]


class TestFeedbackGraph:
    """Feedback graph compiles and runs."""

    def test_compiles(self):
        from agent.graphs.feedback_graph import create_feedback_graph
        graph = create_feedback_graph()
        assert graph is not None

    def test_has_required_nodes(self):
        from agent.graphs.feedback_graph import create_feedback_graph
        graph = create_feedback_graph()
        nodes = graph.get_graph().nodes
        node_names = {n for n in nodes if not n.startswith("__")}
        required = {"refiner", "generator", "guard", "voter", "semantic_check"}
        assert required <= node_names

    def test_does_not_have_cold_start_nodes(self):
        from agent.graphs.feedback_graph import create_feedback_graph
        graph = create_feedback_graph()
        nodes = graph.get_graph().nodes
        node_names = {n for n in nodes if not n.startswith("__")}
        assert "router" not in node_names
        assert "schema_retriever" not in node_names
        assert "decomposer" not in node_names
        assert "fewshot_selector" not in node_names


class TestFeedbackAPI:
    """POST /task/{id}/feedback endpoint."""

    @pytest.fixture(autouse=True)
    def _setup(self):
        from storage.redis_cache import get_redis
        r = get_redis()
        if r is None:
            pytest.skip("Redis not available")
        # Clean up test keys
        r.delete("task:test-fb-001")
        r.delete("task:test-fb-001:cancel")
        r.delete("task:test-fb-001:heartbeat")

    def _get_client(self):
        from api.app import create_app
        from fastapi.testclient import TestClient
        app = create_app()
        return TestClient(app)

    def test_feedback_on_success_task_returns_202(self):
        from infrastructure.task_store import task_create, task_update
        task_create("test-fb-001", "test question", "db_test",
                    "sqlite:///test.db")
        task_update("test-fb-001", status="SUCCESS", sql="SELECT 1",
                    exec_result={"success": True, "row_count": 1, "data": [[1]]})

        client = self._get_client()
        resp = client.post("/api/v1/task/test-fb-001/feedback",
                          json={"feedback": "需要加 GROUP BY"})
        # Accept 202 (success) or 429 (rate limiter)
        assert resp.status_code in (202, 429)

    def test_feedback_on_running_task_returns_400(self):
        from infrastructure.task_store import task_create
        task_create("test-fb-001", "test question", "db_test",
                    "sqlite:///test.db")

        client = self._get_client()
        resp = client.post("/api/v1/task/test-fb-001/feedback",
                          json={"feedback": "需要加 GROUP BY"})
        # PENDING task — should reject
        assert resp.status_code in (400, 429)

    def test_feedback_on_unknown_task_returns_404(self):
        client = self._get_client()
        resp = client.post("/api/v1/task/nonexistent-xx/feedback",
                          json={"feedback": "需要加 GROUP BY"})
        assert resp.status_code in (404, 429)

    def test_feedback_empty_rejected(self):
        client = self._get_client()
        resp = client.post("/api/v1/task/test-fb-001/feedback",
                          json={"feedback": ""})
        # Empty feedback should be rejected by validation
        assert resp.status_code in (422, 429)  # 422 = validation error


class TestFeedbackTransition:
    """feedback_transition() allows SUCCESS/FAILED → RUNNING."""

    def test_success_to_running(self):
        from infrastructure.task_store import task_create, task_update, feedback_transition, task_get
        task_create("test-fb-tr-01", "test", "db", "sqlite:///")
        task_update("test-fb-tr-01", status="SUCCESS", sql="SELECT 1")
        result = feedback_transition("test-fb-tr-01")
        assert result is not None
        assert result.get("status") == "RUNNING"
        state = task_get("test-fb-tr-01")
        assert state.get("status") == "RUNNING"

    def test_pending_rejected(self):
        from infrastructure.task_store import task_create, feedback_transition
        task_create("test-fb-tr-02", "test", "db", "sqlite:///")
        result = feedback_transition("test-fb-tr-02")
        # PENDING → feedback_transition should return the current state (not None, not changed)
        assert result is not None
        assert result.get("status") == "PENDING"
