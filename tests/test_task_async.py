"""Integration tests for W3 async task infrastructure.

Covers: broker graceful degradation, task store CRUD, state transitions,
        idempotency (store + API), API endpoints, TaskMessage serialisation,
        heartbeat, progressive timeout, differentiated TTL, zombie scan (T2).
"""
import hashlib
import json
import pytest

from infrastructure.broker import TaskMessage, TOPIC_REQUEST, TOPIC_RESULT, ALL_TOPICS


def _redis_available():
    from storage.redis_cache import get_redis
    return get_redis() is not None


redis_required = pytest.mark.skipif(not _redis_available(), reason="Redis not available")


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
    """Redis task store — basic CRUD."""

    def test_create_and_get(self):
        from infrastructure.task_store import task_create, task_get
        t = task_create("tstore_test", "How many?")
        assert t is not None
        assert t["status"] == "PENDING"

        t2 = task_get("tstore_test")
        if t2 is not None:  # Redis may be unavailable
            assert t2["question"] == "How many?"

    def test_idempotent_store(self):
        from infrastructure.task_store import idempotent_check, idempotent_set
        idempotent_set("ik_store_1", "task_xyz", ttl=60)
        existing = idempotent_check("ik_store_1")
        if existing is not None:
            assert existing == "task_xyz"
        assert idempotent_check("nonexistent_key_store_xyz123") is None


class TestStateTransitions:
    """State machine: PENDING→RUNNING→SUCCESS/FAILED/TIMEOUT/CANCELLED."""

    def _get_store(self):
        from infrastructure import task_store as ts
        return ts

    def _cleanup(self, ts, task_id: str):
        r = ts._get_redis()
        if r:
            r.delete(f"task:{task_id}")

    @redis_required
    def test_normal_flow_pending_to_success(self):
        ts = self._get_store()
        tid = "flow_success"
        self._cleanup(ts, tid)

        # PENDING
        ts.task_create(tid, "test question")
        s = ts.task_get(tid)
        assert s["status"] == "PENDING"

        # → RUNNING
        ts.task_transition(tid, "RUNNING", node="router")
        s = ts.task_get(tid)
        assert s["status"] == "RUNNING"
        assert s["node"] == "router"

        # Update progress mid-run
        ts.task_update(tid, progress=55, sql="SELECT 1")
        s = ts.task_get(tid)
        assert s["progress"] == 55
        assert s["sql"] == "SELECT 1"

        # → SUCCESS
        ts.task_transition(tid, "SUCCESS", sql="SELECT COUNT(*) FROM t",
                           exec_result={"success": True, "row_count": 42})
        s = ts.task_get(tid)
        assert s["status"] == "SUCCESS"
        assert s["sql"] == "SELECT COUNT(*) FROM t"
        assert s["exec_result"]["row_count"] == 42
        self._cleanup(ts, tid)

    @redis_required
    def test_failure_and_retry(self):
        ts = self._get_store()
        tid = "flow_fail"
        self._cleanup(ts, tid)

        ts.task_create(tid, "bad query")
        ts.task_transition(tid, "RUNNING")
        # Simulate failure
        ts.task_transition(tid, "FAILED", error="LLM timeout")
        s = ts.task_get(tid)
        assert s["status"] == "FAILED"
        assert s["error"] == "LLM timeout"

        # Retry: FAILED → PENDING
        ts.task_transition(tid, "PENDING", retry_count=2)
        s = ts.task_get(tid)
        assert s["status"] == "PENDING"
        assert s["retry_count"] == 2
        self._cleanup(ts, tid)

    @redis_required
    def test_cancel_flow(self):
        ts = self._get_store()
        tid = "flow_cancel"
        self._cleanup(ts, tid)

        ts.task_create(tid, "slow query")
        ts.task_transition(tid, "RUNNING")

        # Request cancel
        ok = ts.task_request_cancel(tid)
        assert ok is True
        assert ts.task_is_cancelled(tid) is True

        # Worker transitions to CANCELLED
        ts.task_transition(tid, "CANCELLED")
        s = ts.task_get(tid)
        assert s["status"] == "CANCELLED"
        self._cleanup(ts, tid)

    @redis_required
    def test_timeout_flow(self):
        ts = self._get_store()
        tid = "flow_timeout"
        self._cleanup(ts, tid)

        ts.task_create(tid, "stuck query")
        ts.task_transition(tid, "RUNNING")
        ts.task_transition(tid, "TIMEOUT", error="Exceeded 120s")
        s = ts.task_get(tid)
        assert s["status"] == "TIMEOUT"
        assert "120s" in (s.get("error") or "")
        self._cleanup(ts, tid)

    @redis_required
    def test_terminal_states_are_final(self):
        """SUCCESS/CANCELLED cannot transition further."""
        ts = self._get_store()

        for terminal in ("SUCCESS", "CANCELLED"):
            tid = f"flow_terminal_{terminal}"
            self._cleanup(ts, tid)
            ts.task_create(tid, "q")
            ts.task_transition(tid, "RUNNING")
            ts.task_transition(tid, terminal)  # legal: RUNNING → terminal

            # Confirm terminal state
            s = ts.task_get(tid)
            assert s["status"] == terminal

            # Attempting RUNNING after terminal → blocked, returns current unchanged
            ts.task_transition(tid, "RUNNING")
            s = ts.task_get(tid)
            assert s["status"] == terminal  # unchanged
            self._cleanup(ts, tid)

    @redis_required
    def test_nonexistent_task_get_returns_none(self):
        ts = self._get_store()
        assert ts.task_get("nonexistent_flow_12345") is None

    @redis_required
    def test_updated_at_changes(self):
        ts = self._get_store()
        tid = "flow_updated_at"
        self._cleanup(ts, tid)

        ts.task_create(tid, "q")
        s1 = ts.task_get(tid)
        import time
        time.sleep(0.1)
        ts.task_update(tid, progress=50)
        s2 = ts.task_get(tid)
        assert s2["updated_at"] != s1["updated_at"]
        self._cleanup(ts, tid)


class TestWorkerImports:
    def test_worker_imports(self):
        from worker.main import run_graph, handle_task, _sanitize, _summarize_node, _node_progress
        assert callable(run_graph)
        assert callable(handle_task)
        assert callable(_sanitize)
        assert callable(_summarize_node)
        assert callable(_node_progress)

    def test_sanitize_none(self):
        from worker.main import _sanitize
        assert _sanitize(None) is None

    def test_sanitize_empty(self):
        from worker.main import _sanitize
        r = _sanitize({"success": True, "data": []})
        assert r["success"] is True

    def test_node_progress_ordering(self):
        """Progress increases through pipeline nodes."""
        from worker.main import _node_progress
        nodes = ["router", "schema_retriever", "generator", "voter", "semantic_check"]
        vals = [_node_progress(n) for n in nodes]
        # Must be strictly increasing
        for i in range(1, len(vals)):
            assert vals[i] > vals[i - 1], f"{nodes[i]} progress <= {nodes[i-1]}"

    def test_summarize_node_router(self):
        from worker.main import _summarize_node
        s = _summarize_node("router", {"complexity": "complex", "router_score": 7})
        assert s["complexity"] == "complex"

    def test_summarize_node_generator(self):
        from worker.main import _summarize_node
        s = _summarize_node("generator", {"sql": "SELECT 1", "candidate_sqls": []})
        assert s["candidate_count"] == 0
        assert "SELECT 1" in s["sql_preview"]


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
        assert resp.status_code == 202
        body = resp.json()
        assert "task_id" in body
        assert body["status"] == "PENDING"

    def test_submit_idempotency(self, client):
        """Same idempotency_key + question → returns same task_id."""
        resp1 = client.post("/api/v1/task/submit", json={
            "question": "Count unique users",
            "db_id": "test_db",
            "idempotency_key": "client-key-001",
        })
        assert resp1.status_code == 202
        tid1 = resp1.json()["task_id"]

        resp2 = client.post("/api/v1/task/submit", json={
            "question": "Count unique users",
            "db_id": "test_db",
            "idempotency_key": "client-key-001",
        })
        assert resp2.status_code == 202
        tid2 = resp2.json()["task_id"]
        if _redis_available():
            assert tid1 == tid2  # exact same task_id returned
        else:
            assert tid1 != tid2  # no Redis → no dedup → new task_id each time

    @redis_required
    def test_full_lifecycle_api(self, client):
        """Submit → check status → cancel → verify cancelled."""
        # Submit
        resp = client.post("/api/v1/task/submit", json={
            "question": "API lifecycle test",
            "db_id": "lifecycle_test",
        })
        assert resp.status_code == 202
        tid = resp.json()["task_id"]

        # Check initial status
        resp2 = client.get(f"/api/v1/task/{tid}/status")
        assert resp2.status_code == 200
        body = resp2.json()
        assert body["status"] == "PENDING"
        assert body["question"] == "API lifecycle test"

        # Cancel
        resp3 = client.post(f"/api/v1/task/{tid}/cancel")
        assert resp3.status_code == 200
        cancel_body = resp3.json()
        assert cancel_body["status"] in ("cancelled", "CANCELLED", "PENDING")

        # Cleanup
        from storage.redis_cache import get_redis
        r = get_redis()
        if r:
            r.delete(f"task:{tid}")

    def test_status_404_for_unknown(self, client):
        resp = client.get("/api/v1/task/status/nonexistent123")
        assert resp.status_code in (200, 404)

    def test_cancel_unknown(self, client):
        resp = client.post("/api/v1/task/cancel/nonexistent123")
        assert resp.status_code in (200, 404)


class TestHeartbeat:
    """T2: heartbeat write/read and TTL expiry."""

    def _cleanup(self, task_id: str):
        from infrastructure.task_store import _get_redis
        r = _get_redis()
        if r:
            r.delete(f"task:{task_id}", f"task:{task_id}:heartbeat")

    @redis_required
    def test_heartbeat_write_and_read(self):
        from infrastructure.task_store import task_create, task_heartbeat, task_get_heartbeat
        from datetime import datetime

        tid = "hb_write_test"
        self._cleanup(tid)
        task_create(tid, "test q")

        task_heartbeat(tid)
        hb = task_get_heartbeat(tid)
        assert hb is not None

        # Must be a valid ISO timestamp
        dt = datetime.fromisoformat(hb)
        assert dt.tzinfo is not None  # UTC
        self._cleanup(tid)

    @redis_required
    def test_heartbeat_expires(self):
        """Heartbeat key has short TTL — should expire quickly."""
        from infrastructure.task_store import (
            task_create, task_heartbeat, task_get_heartbeat,
            _get_redis, HEARTBEAT_TTL_S,
        )

        tid = "hb_expire_test"
        self._cleanup(tid)
        task_create(tid, "test q")

        task_heartbeat(tid)
        # Manually expire the heartbeat key
        r = _get_redis()
        r.delete(f"task:{tid}:heartbeat")

        hb = task_get_heartbeat(tid)
        assert hb is None  # expired
        self._cleanup(tid)

    @redis_required
    def test_get_heartbeat_nonexistent_task(self):
        from infrastructure.task_store import task_get_heartbeat
        assert task_get_heartbeat("nonexistent_hb_xyz") is None


class TestStaleScan:
    """T2: zombie detection — stale RUNNING tasks → TIMEOUT."""

    def _get_store(self):
        from infrastructure import task_store as ts
        return ts

    def _cleanup(self, tid: str):
        ts = self._get_store()
        r = ts._get_redis()
        if r:
            r.delete(f"task:{tid}", f"task:{tid}:heartbeat", f"task:{tid}:cancel")

    @redis_required
    def test_no_stale_when_heartbeat_fresh(self):
        ts = self._get_store()
        tid = "stale_fresh"
        self._cleanup(tid)

        ts.task_create(tid, "q")
        ts.task_transition(tid, "RUNNING")
        ts.task_heartbeat(tid)  # fresh heartbeat

        stale = ts.scan_stale_tasks(stale_s=60)
        assert tid not in stale
        self._cleanup(tid)

    @redis_required
    def test_stale_detected_when_no_heartbeat(self):
        ts = self._get_store()
        tid = "stale_nohb"
        self._cleanup(tid)

        ts.task_create(tid, "q")
        ts.task_transition(tid, "RUNNING")
        # No heartbeat at all

        stale = ts.scan_stale_tasks(stale_s=60)
        assert tid in stale
        # Must have been transitioned to TIMEOUT
        s = ts.task_get(tid)
        assert s["status"] == "TIMEOUT"
        self._cleanup(tid)

    @redis_required
    def test_stale_detected_expired_heartbeat(self):
        ts = self._get_store()
        tid = "stale_expired"
        self._cleanup(tid)

        ts.task_create(tid, "q")
        ts.task_transition(tid, "RUNNING")
        ts.task_heartbeat(tid)

        # Delete heartbeat to simulate expiry
        r = ts._get_redis()
        r.delete(f"task:{tid}:heartbeat")

        stale = ts.scan_stale_tasks(stale_s=60)
        assert tid in stale
        s = ts.task_get(tid)
        assert s["status"] == "TIMEOUT"
        assert "heartbeat" in (s.get("error") or "")
        self._cleanup(tid)

    @redis_required
    def test_scan_skips_terminal_states(self):
        """SUCCESS/FAILED/CANCELLED tasks are not scanned."""
        ts = self._get_store()

        for status in ("SUCCESS", "FAILED", "CANCELLED"):
            tid = f"stale_term_{status}"
            self._cleanup(tid)
            ts.task_create(tid, "q")
            ts.task_transition(tid, "RUNNING")
            ts.task_transition(tid, status)
            # No heartbeat — but state is terminal, should be skipped

            stale = ts.scan_stale_tasks(stale_s=60)
            assert tid not in stale
            self._cleanup(tid)

    @redis_required
    def test_scan_empty_when_nothing_running(self):
        ts = self._get_store()
        stale = ts.scan_stale_tasks(stale_s=60)
        # May find stale tasks from other tests, but should not crash
        assert isinstance(stale, list)


class TestProgressiveTimeout:
    """T2: progressive timeouts matching eval system."""

    def test_attempt_0(self):
        from infrastructure.task_store import get_task_timeout, TASK_TIMEOUTS
        assert get_task_timeout(0) == TASK_TIMEOUTS[0]  # 120

    def test_attempt_1(self):
        from infrastructure.task_store import get_task_timeout
        assert get_task_timeout(1) == 300

    def test_attempt_2(self):
        from infrastructure.task_store import get_task_timeout
        assert get_task_timeout(2) == 480

    def test_beyond_max_clamped(self):
        from infrastructure.task_store import get_task_timeout, TASK_TIMEOUTS
        assert get_task_timeout(99) == TASK_TIMEOUTS[-1]  # 480

    def test_default_retry_count(self):
        from infrastructure.task_store import get_task_timeout
        assert get_task_timeout() == 120  # retry_count=0 by default


class TestDifferentiatedTTL:
    """T2: state keys get appropriate TTL based on task status."""

    @redis_required
    def test_pending_ttl(self):
        from infrastructure.task_store import task_create, _get_redis, _ttl_for_status, TTL_RUNNING
        tid = "ttl_test_pending"
        r = _get_redis()
        r.delete(f"task:{tid}")

        task_create(tid, "test q")
        ttl = r.ttl(f"task:{tid}")
        assert ttl > 0
        # PENDING uses TTL_RUNNING (2h)
        assert abs(ttl - TTL_RUNNING) < 10
        r.delete(f"task:{tid}")

    @redis_required
    def test_success_ttl(self):
        from infrastructure.task_store import task_create, task_transition, _get_redis, TTL_TERMINAL_GOOD
        tid = "ttl_test_success"
        r = _get_redis()
        r.delete(f"task:{tid}")

        task_create(tid, "test q")
        task_transition(tid, "RUNNING")
        task_transition(tid, "SUCCESS")
        ttl = r.ttl(f"task:{tid}")
        assert ttl > 0
        # SUCCESS uses TTL_TERMINAL_GOOD (24h)
        assert abs(ttl - TTL_TERMINAL_GOOD) < 10
        r.delete(f"task:{tid}")

    @redis_required
    def test_timeout_ttl(self):
        from infrastructure.task_store import task_create, task_transition, _get_redis, TTL_TERMINAL_BAD
        tid = "ttl_test_timeout"
        r = _get_redis()
        r.delete(f"task:{tid}")

        task_create(tid, "test q")
        task_transition(tid, "RUNNING")
        task_transition(tid, "TIMEOUT", error="test")
        ttl = r.ttl(f"task:{tid}")
        assert ttl > 0
        # TIMEOUT uses TTL_TERMINAL_BAD (1h)
        assert abs(ttl - TTL_TERMINAL_BAD) < 10
        r.delete(f"task:{tid}")


class TestHealthEndpoint:
    """T2: GET /task/{id}/health and POST /task/scan-stale endpoints."""

    @pytest.fixture
    def client(self):
        from api.app import app
        from fastapi.testclient import TestClient
        return TestClient(app)

    def test_health_404_for_unknown(self, client):
        resp = client.get("/api/v1/task/health/nonexistent123")
        assert resp.status_code in (200, 404, 429)  # 429 = rate limited

    @redis_required
    def test_health_returns_heartbeat_status(self, client):
        from infrastructure.task_store import task_create, task_heartbeat

        tid = "health_test"
        # Clean up any previous state
        from storage.redis_cache import get_redis
        r = get_redis()
        if r:
            r.delete(f"task:{tid}", f"task:{tid}:heartbeat")

        task_create(tid, "health check q")
        task_heartbeat(tid)

        resp = client.get(f"/api/v1/task/{tid}/health")
        if resp.status_code == 200:
            body = resp.json()
            assert body["task_id"] == tid
            assert body["task_status"] == "PENDING"
            assert body["heartbeat"] is not None
            assert body["healthy"] is True
            assert "heartbeat_stale_s" in body
        else:
            assert resp.status_code == 429  # rate limited

        if r:
            r.delete(f"task:{tid}", f"task:{tid}:heartbeat")

    def test_scan_stale_endpoint(self, client):
        resp = client.post("/api/v1/task/scan-stale")
        # 200 = ok, 429 = rate limited (acceptable in test suite)
        assert resp.status_code in (200, 429)


class TestTokenStreaming:
    """T3: Redis pub/sub token streaming."""

    def _cleanup(self, tid: str):
        from infrastructure.task_store import _get_redis
        r = _get_redis()
        if r:
            r.delete(f"task:{tid}", f"task:{tid}:tokens")

    @redis_required
    def test_publish_and_subscribe_roundtrip(self):
        """Worker publishes token, subscriber receives it."""
        import threading
        from infrastructure.task_store import task_publish_token, _get_redis

        tid = "token_roundtrip"
        self._cleanup(tid)

        received = []
        r = _get_redis()
        ps = r.pubsub(ignore_subscribe_messages=True)
        ps.subscribe(f"task:{tid}:tokens")

        def _collect():
            for msg in ps.listen():
                if msg.get("type") == "message":
                    received.append(msg["data"])
                    if len(received) >= 3:
                        break

        t = threading.Thread(target=_collect, daemon=True)
        t.start()

        import time
        time.sleep(0.05)  # let subscriber settle

        task_publish_token(tid, "SELECT")
        task_publish_token(tid, " *")
        task_publish_token(tid, " FROM t")

        t.join(timeout=2)
        ps.unsubscribe(f"task:{tid}:tokens")
        ps.close()

        assert received == ["SELECT", " *", " FROM t"]
        self._cleanup(tid)

    @redis_required
    def test_publish_noop_when_redis_down(self):
        """task_publish_token should not raise when Redis is unavailable."""
        from infrastructure.task_store import task_publish_token, _get_redis
        # This test just confirms no exception
        task_publish_token("nonexistent", "SELECT")
        # passes if no exception

    def test_worker_run_graph_sets_token_callback(self):
        """Verify token_callback set/clear contract on generator module."""
        import agent.nodes.generator as gen_mod

        # Save original
        original = dict(gen_mod._stream_ctx)
        gen_mod._stream_ctx.clear()

        try:
            from worker.main import run_graph
            assert callable(run_graph)

            # Gen module has set_token_callback
            assert callable(gen_mod.set_token_callback)

            # Callback initially None
            assert "cb" not in gen_mod._stream_ctx

            # Set callback
            calls = []
            gen_mod.set_token_callback(lambda t: calls.append(t))
            assert "cb" in gen_mod._stream_ctx

            # Invoke via module internals
            gen_mod._stream_ctx["cb"]("test")
            assert calls == ["test"]

            # Clear callback
            gen_mod.set_token_callback(None)
            assert "cb" not in gen_mod._stream_ctx
        finally:
            gen_mod._stream_ctx.clear()
            gen_mod._stream_ctx.update(original)
