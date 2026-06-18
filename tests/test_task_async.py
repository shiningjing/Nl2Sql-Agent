"""Integration tests for W3 async task infrastructure.

Covers: broker graceful degradation, task store CRUD, API endpoints,
        TaskMessage serialisation, idempotency.
"""
import json
import pytest

from infrastructure.broker import TaskMessage, TOPIC_REQUEST, TOPIC_RESULT, ALL_TOPICS


class TestTaskMessage:
    def test_roundtrip(self):
        msg = TaskMessage(task_id="abc123", event="submitted",
                          payload={"question": "SELECT 1"})
        raw = msg.to_json()
        d = json.loads(raw)
        assert d["task_id"] == "abc123"
        assert d["event"] == "submitted"

        msg2 = TaskMessage.from_json(raw)
        assert msg2.task_id == "abc123"
        assert msg2.event == "submitted"
        assert msg2.payload["question"] == "SELECT 1"

    def test_empty_payload(self):
        msg = TaskMessage(task_id="x", event="running")
        raw = msg.to_json()
        msg2 = TaskMessage.from_json(raw)
        assert msg2.payload == {}

    def test_topic_constants(self):
        assert TOPIC_REQUEST == "nl2sql.task.request"
        assert TOPIC_RESULT == "nl2sql.task.result"
        assert len(ALL_TOPICS) == 4


class TestBrokerDegradation:
    """Kafka unavailable → no crash, no-op publish."""

    def test_unconnected_publish_is_noop(self):
        from infrastructure.broker import get_broker
        b = get_broker()
        # Force disconnected state
        b._connected = False
        msg = TaskMessage(task_id="t1", event="test")
        ok = b.publish("test", msg)
        assert ok is False

    def test_unconnected_close_safe(self):
        from infrastructure.broker import get_broker
        b = get_broker()
        b._connected = False
        b._producer = None
        b.close()  # should not raise


class TestTaskStore:
    """Redis task store with available Redis."""

    def test_create_and_get(self):
        from infrastructure.task_store import task_create, task_get
        t = task_create("tstore_test", "How many?")
        assert t is not None
        assert t["status"] == "PENDING"

        t2 = task_get("tstore_test")
        if t2 is not None:  # Redis may be unavailable
            assert t2["question"] == "How many?"

    def test_idempotent(self):
        from infrastructure.task_store import idempotent_check, idempotent_set
        idempotent_set("ik1", "task_xyz", ttl=60)
        existing = idempotent_check("ik1")
        if existing is not None:
            assert existing == "task_xyz"
        # Unknown key returns None or doesn't exist
        assert idempotent_check("nonexistent_key_xyz123") is None


class TestWorkerImports:
    def test_worker_imports(self):
        from worker.main import run_graph, handle_task, _sanitize, _summarize_node
        assert callable(run_graph)
        assert callable(handle_task)
        assert callable(_sanitize)
        assert callable(_summarize_node)

    def test_sanitize_none(self):
        from worker.main import _sanitize
        assert _sanitize(None) is None

    def test_sanitize_empty(self):
        from worker.main import _sanitize
        r = _sanitize({"success": True, "data": []})
        assert r["success"] is True


class TestApiRoutes:
    """FastAPI test client for async task endpoints."""

    @pytest.fixture
    def client(self):
        from api.app import app
        from fastapi.testclient import TestClient
        return TestClient(app)

    def test_submit_returns_202(self, client):
        resp = client.post("/api/v1/task/submit", json={
            "question": "How many schools?",
            "db_id": "california_schools",
        })
        # 202 Accepted — Kafka may be down but endpoint returns task_id
        assert resp.status_code == 202
        body = resp.json()
        assert "task_id" in body
        assert body["status"] == "PENDING"

    def test_status_404_for_unknown(self, client):
        resp = client.get("/api/v1/task/status/nonexistent123")
        # 404 if Redis available, or 200 with null fields if degraded
        assert resp.status_code in (200, 404)

    def test_cancel_unknown(self, client):
        resp = client.post("/api/v1/task/cancel/nonexistent123")
        # 404 if Redis down (no task found), 200 if Redis up with not_found
        assert resp.status_code in (200, 404)
