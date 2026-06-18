"""Tests for MCP execute_readonly_sql tool.

Covers: valid SELECT, INSERT/UPDATE/DELETE/DROP rejection, multi-statement,
        LIMIT auto-wrap, max_rows hard cap, timeout, empty SQL, CTE, JOIN.
"""

import os
import pytest

from tools.mcp.execute_readonly_server import execute_readonly_sql

BIRD_BASE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "bird", "mini_dev_data", "minidev", "MINIDEV", "dev_databases",
)
SQLITE_DB = f"sqlite:///{BIRD_BASE}/california_schools/california_schools.sqlite"


class TestExecuteReadOnly:
    """Direct function tests (bypass MCP transport)."""

    # ── Happy path: SQLite ─────────────────────────────────────────────

    def test_simple_select(self):
        r = execute_readonly_sql("SELECT * FROM schools", SQLITE_DB)
        assert r["success"] is True
        assert r["row_count"] > 0
        assert r["columns"] is not None
        assert r["data"] is not None
        assert r["error"] is None

    def test_select_with_count(self):
        r = execute_readonly_sql("SELECT COUNT(*) AS cnt FROM schools", SQLITE_DB)
        assert r["success"] is True
        assert r["row_count"] == 1

    def test_cte_select(self):
        r = execute_readonly_sql(
            "WITH s AS (SELECT * FROM schools) SELECT * FROM s",
            SQLITE_DB,
        )
        assert r["success"] is True

    # ── LIMIT auto-wrap ────────────────────────────────────────────────

    def test_auto_limit_applied(self):
        r = execute_readonly_sql("SELECT * FROM schools", SQLITE_DB, max_rows=5)
        assert r["success"] is True
        assert r["row_count"] <= 5

    def test_existing_limit_respected(self):
        r = execute_readonly_sql(
            "SELECT * FROM schools LIMIT 3",
            SQLITE_DB,
            max_rows=200,
        )
        assert r["success"] is True
        assert r["row_count"] <= 3

    # ── max_rows hard cap ──────────────────────────────────────────────

    def test_max_rows_exceeds_hard_cap_rejected(self):
        r = execute_readonly_sql(
            "SELECT * FROM schools",
            SQLITE_DB,
            max_rows=2000,
        )
        assert r["success"] is False
        assert r["error_type"] == "INVALID_INPUT"

    # ── Empty SQL ──────────────────────────────────────────────────────

    def test_empty_sql(self):
        r = execute_readonly_sql("", SQLITE_DB)
        assert r["success"] is False
        assert r["error_type"] == "INVALID_INPUT"

    def test_whitespace_sql(self):
        r = execute_readonly_sql("   ", SQLITE_DB)
        assert r["success"] is False

    # ── Security: forbidden statement types ────────────────────────────

    def test_insert_rejected(self):
        r = execute_readonly_sql("INSERT INTO schools VALUES (1)", SQLITE_DB)
        assert r["success"] is False

    def test_update_rejected(self):
        r = execute_readonly_sql("UPDATE schools SET name = 'x'", SQLITE_DB)
        assert r["success"] is False

    def test_delete_rejected(self):
        r = execute_readonly_sql("DELETE FROM schools", SQLITE_DB)
        assert r["success"] is False

    def test_drop_rejected(self):
        r = execute_readonly_sql("DROP TABLE schools", SQLITE_DB)
        assert r["success"] is False

    def test_create_rejected(self):
        r = execute_readonly_sql("CREATE TABLE t (id INT)", SQLITE_DB)
        assert r["success"] is False

    # ── Security: multi-statement ─────────────────────────────────────

    def test_multi_statement_rejected(self):
        r = execute_readonly_sql(
            "SELECT * FROM schools; SELECT * FROM schools",
            SQLITE_DB,
        )
        assert r["success"] is False

    # ── Syntax error ──────────────────────────────────────────────────

    def test_syntax_error(self):
        r = execute_readonly_sql("SELECT * FROM WHERE WHERE x = 1", SQLITE_DB)
        assert r["success"] is False

    # ── Columns and data format ───────────────────────────────────────

    def test_columns_returned(self):
        r = execute_readonly_sql("SELECT School, County FROM schools LIMIT 2", SQLITE_DB)
        assert r["success"] is True
        assert "School" in r["columns"] or "school" in [c.lower() for c in r["columns"]]

    def test_data_is_list_of_lists(self):
        r = execute_readonly_sql("SELECT * FROM schools LIMIT 2", SQLITE_DB)
        assert r["success"] is True
        assert isinstance(r["data"], list)
        assert isinstance(r["data"][0], list)

    def test_execution_ms_returned(self):
        r = execute_readonly_sql("SELECT 1", SQLITE_DB)
        assert r["execution_ms"] > 0

    # ── Nonexistent table ─────────────────────────────────────────────

    def test_nonexistent_table(self):
        r = execute_readonly_sql(
            "SELECT * FROM nonexistent_table_xyz",
            SQLITE_DB,
        )
        assert r["success"] is False
        assert r["error_type"] == "EXECUTION_ERROR"

    # ── Default parameters ────────────────────────────────────────────

    def test_default_max_rows(self):
        r = execute_readonly_sql("SELECT * FROM schools", SQLITE_DB)
        assert r["success"] is True
        assert r["row_count"] <= 200
