"""Integration tests for FastAPI endpoints — 4 routes + X-Trace-ID header."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    from api.app import app
    return TestClient(app)


class TestHealthEndpoint:
    def test_health_returns_200(self, client):
        resp = client.get("/api/v1/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] in ("healthy", "degraded")
        assert data["db"] in ("ok", "error")
        assert data["redis"] in ("ok", "unavailable")
        assert "version" in data
        assert "timestamp" in data

    def test_health_response_has_trace_id(self, client):
        resp = client.get("/api/v1/health")
        assert "X-Trace-ID" in resp.headers
        assert len(resp.headers["X-Trace-ID"]) > 0

    def test_schema_returns_ddl_and_tables(self, client):
        resp = client.get("/api/v1/schema")
        assert resp.status_code == 200
        data = resp.json()
        assert "ddl" in data
        assert "tables" in data
        assert len(data["tables"]) > 0

    def test_schema_response_has_trace_id(self, client):
        resp = client.get("/api/v1/schema")
        assert "X-Trace-ID" in resp.headers


class TestQueryMiniEndpoint:
    def test_query_validation_empty_question(self, client):
        resp = client.post("/api/v1/query", json={"question": ""})
        assert resp.status_code == 422  # validation error

    def test_query_validation_missing_question(self, client):
        resp = client.post("/api/v1/query", json={})
        assert resp.status_code == 422

    def test_query_returns_response_structure(self, client):
        """End-to-end Mini pipeline test — requires LLM API access."""
        resp = client.post("/api/v1/query", json={
            "question": "How many customers are there?",
            "reviewer_on": False,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "request_id" in data
        assert "question" in data
        assert "sql" in data
        assert data["sql"] != ""
        assert "cache_hit" in data
        assert "elapsed_ms" in data

    def test_query_has_trace_id(self, client):
        resp = client.post("/api/v1/query", json={
            "question": "Count all customers",
            "reviewer_on": False,
        })
        assert resp.status_code == 200
        assert "X-Trace-ID" in resp.headers


class TestQueryFullEndpoint:
    def test_query_full_validation_empty_question(self, client):
        resp = client.post("/api/v1/query/full", json={"question": ""})
        assert resp.status_code == 422

    def test_query_full_returns_response_structure(self, client):
        """End-to-end Full Graph test — requires LLM API access."""
        resp = client.post("/api/v1/query/full", json={
            "question": "Count total customers",
            "multi_candidate": False,
            "rag_schema": False, "rag_domain": False,  # no db_id for RAG
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "request_id" in data
        assert "sql" in data
        assert data["sql"] != ""

    def test_query_full_has_trace_id(self, client):
        resp = client.post("/api/v1/query/full", json={
            "question": "Count all orders",
            "multi_candidate": False,
            "rag_schema": False, "rag_domain": False,
        })
        assert resp.status_code == 200
        assert "X-Trace-ID" in resp.headers


class TestRateLimitMiddleware:
    def test_regular_request_passes(self, client):
        resp = client.get("/api/v1/health")
        assert resp.status_code == 200
