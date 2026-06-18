"""Tests for guard/safety_rules.py — check_safety + check_hallucinations."""

import os
import pytest

from guard.safety_rules import check_safety, check_hallucinations

BIRD_BASE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "bird", "mini_dev_data", "minidev", "MINIDEV", "dev_databases",
)
SQLITE_DB = f"sqlite:///{BIRD_BASE}/california_schools/california_schools.sqlite"


def _get_schema() -> dict:
    from retrieval.schema import _get_cached_schema_info
    return _get_cached_schema_info(SQLITE_DB)


# ── check_safety ──────────────────────────────────────────────────────

class TestCheckSafety:

    def test_valid_select(self):
        r = check_safety("SELECT * FROM schools")
        assert r["valid"] is True
        assert r["issues"] == []
        assert r["statement_type"] == "SELECT"
        assert "schools" in r["table_references"]

    def test_cte_filtered_from_tables(self):
        r = check_safety(
            "WITH x AS (SELECT * FROM t) SELECT * FROM x"
        )
        assert r["valid"] is True
        assert "x" not in r["table_references"]
        assert "t" in r["table_references"]

    def test_forbidden_insert(self):
        r = check_safety("INSERT INTO t VALUES (1)")
        assert r["valid"] is False
        assert any(i["type"] == "forbidden_keyword" for i in r["issues"])

    def test_multi_statement(self):
        r = check_safety("SELECT 1; SELECT 2")
        assert r["valid"] is False
        assert any(i["type"] == "multi_statement" for i in r["issues"])

    def test_syntax_error(self):
        r = check_safety("SELECT * FROM WHERE")
        assert r["valid"] is False
        assert any(i["type"] == "ast_syntax" for i in r["issues"])

    def test_empty_sql(self):
        r = check_safety("")
        assert r["valid"] is False
        assert any(i["type"] == "empty_sql" for i in r["issues"])

    def test_postgres_dialect(self):
        r = check_safety("SELECT * FROM orders", dialect="postgres")
        assert r["valid"] is True

    def test_missing_select(self):
        r = check_safety("SET autocommit = 1")
        assert r["valid"] is False
        assert any(i["type"] == "missing_select" for i in r["issues"])


# ── check_hallucinations ─────────────────────────────────────────────

class TestCheckHallucinations:

    @pytest.fixture(autouse=True)
    def schema_info(self):
        try:
            return _get_schema()
        except Exception:
            return {}

    def test_table_exists(self, schema_info):
        if not schema_info:
            pytest.skip("Schema not available")
        r = check_hallucinations("SELECT * FROM schools", schema_info)
        assert r["valid"] is True

    def test_table_not_exists(self, schema_info):
        if not schema_info:
            pytest.skip("Schema not available")
        r = check_hallucinations("SELECT * FROM nonexistent_xyz", schema_info)
        assert r["valid"] is False
        assert any("Table" in i["detail"] for i in r["issues"])

    def test_column_exists(self, schema_info):
        if not schema_info:
            pytest.skip("Schema not available")
        r = check_hallucinations("SELECT School, County FROM schools", schema_info)
        assert r["valid"] is True

    def test_column_not_exists(self, schema_info):
        if not schema_info:
            pytest.skip("Schema not available")
        r = check_hallucinations("SELECT nonexistent_col_xyz FROM schools", schema_info)
        assert r["valid"] is False
        assert any("not in schema" in i["detail"] for i in r["issues"])

    def test_cte_col_trusted(self, schema_info):
        """Columns from CTE aliases should not trigger false positives."""
        if not schema_info:
            pytest.skip("Schema not available")
        r = check_hallucinations(
            "WITH s AS (SELECT * FROM schools) SELECT * FROM s",
            schema_info,
        )
        assert r["valid"] is True

    def test_select_alias_trusted(self, schema_info):
        """SELECT aliases (e.g., COUNT(*) AS cnt) should not trigger false positives."""
        if not schema_info:
            pytest.skip("Schema not available")
        r = check_hallucinations(
            "SELECT COUNT(*) AS cnt FROM schools",
            schema_info,
        )
        assert r["valid"] is True
