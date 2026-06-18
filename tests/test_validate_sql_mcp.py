"""Tests for MCP validate_sql tool.

Covers: valid SELECT, multi-statement, DROP, syntax error, CTE, WITH,
        dialect support, empty input, benign SELECT without issues.
"""

import pytest

from tools.mcp.validate_sql_server import validate_sql


class TestValidateSQL:
    """Direct function tests (bypass MCP transport)."""

    # ── Happy path ───────────────────────────────────────────────────────

    def test_simple_select(self):
        result = validate_sql("SELECT * FROM orders")
        assert result["valid"] is True
        assert result["issues"] == []
        assert result["statement_type"] == "SELECT"
        assert "orders" in result["table_references"]

    def test_with_cte(self):
        result = validate_sql(
            "WITH active AS (SELECT * FROM orders WHERE status = 'active') "
            "SELECT * FROM active"
        )
        assert result["valid"] is True
        assert result["statement_type"] == "SELECT"
        # CTE alias "active" should be filtered from table_references
        assert "active" not in result["table_references"]
        assert "orders" in result["table_references"]

    def test_select_with_joins(self):
        result = validate_sql(
            "SELECT o.id, c.name FROM orders o JOIN customers c ON o.cust_id = c.id"
        )
        assert result["valid"] is True
        assert "orders" in result["table_references"]
        assert "customers" in result["table_references"]

    def test_select_without_from(self):
        result = validate_sql("SELECT 1")
        assert result["valid"] is True
        assert result["statement_type"] == "SELECT"

    # ── L1: forbidden keywords ──────────────────────────────────────────

    def test_forbidden_insert(self):
        result = validate_sql("INSERT INTO orders VALUES (1, 'test')")
        assert result["valid"] is False
        types = [i["type"] for i in result["issues"]]
        assert "forbidden_keyword" in types

    def test_forbidden_drop(self):
        result = validate_sql("DROP TABLE orders")
        assert result["valid"] is False
        types = [i["type"] for i in result["issues"]]
        assert "forbidden_keyword" in types

    def test_forbidden_update(self):
        result = validate_sql("UPDATE orders SET status = 'done'")
        assert result["valid"] is False
        types = [i["type"] for i in result["issues"]]
        assert "forbidden_keyword" in types

    def test_forbidden_delete(self):
        result = validate_sql("DELETE FROM orders WHERE id = 1")
        assert result["valid"] is False

    # ── L1: multi-statement ─────────────────────────────────────────────

    def test_multi_statement(self):
        result = validate_sql("SELECT * FROM orders; SELECT * FROM customers")
        assert result["valid"] is False
        types = [i["type"] for i in result["issues"]]
        assert "multi_statement" in types

    def test_multi_statement_with_drop(self):
        result = validate_sql("SELECT * FROM orders; DROP TABLE orders")
        assert result["valid"] is False
        types = [i["type"] for i in result["issues"]]
        # Should catch both multi_statement AND forbidden_keyword
        assert "multi_statement" in types
        assert "forbidden_keyword" in types

    # ── L1: missing SELECT ──────────────────────────────────────────────

    def test_explain_select_allowed(self):
        """EXPLAIN is read-only and safe — should pass validation."""
        result = validate_sql("EXPLAIN SELECT * FROM orders")
        assert result["valid"] is True

    def test_no_select(self):
        result = validate_sql("SET autocommit = 1")
        assert result["valid"] is False
        types = [i["type"] for i in result["issues"]]
        assert "missing_select" in types

    # ── L1: empty input ─────────────────────────────────────────────────

    def test_empty_sql(self):
        result = validate_sql("")
        assert result["valid"] is False
        types = [i["type"] for i in result["issues"]]
        assert "empty_sql" in types

    def test_whitespace_sql(self):
        result = validate_sql("   \n  \t  ")
        assert result["valid"] is False
        types = [i["type"] for i in result["issues"]]
        assert "empty_sql" in types

    # ── L3: AST syntax errors ───────────────────────────────────────────

    def test_ast_syntax_error(self):
        result = validate_sql("SELECT * FROM WHERE WHERE x = 1")
        assert result["valid"] is False
        # The L1 regex check passes (SELECT present, no forbidden keywords)
        # but sqlglot should catch the syntax error
        types = [i["type"] for i in result["issues"]]
        assert "ast_syntax" in types

    def test_ast_garbled_input(self):
        result = validate_sql("FOO BAR BAZ 123 !!!")
        assert result["valid"] is False
        types = [i["type"] for i in result["issues"]]
        # Both missing_select (no SELECT) and ast_syntax
        assert "missing_select" in types

    # ── L3: AST forbidden (more accurate than regex) ────────────────────

    def test_ast_catches_create_table(self):
        result = validate_sql("CREATE TABLE t (id INT)")
        assert result["valid"] is False

    # ── Dialect support ─────────────────────────────────────────────────

    def test_postgres_dialect(self):
        result = validate_sql(
            "SELECT * FROM orders LIMIT 10",
            dialect="postgres",
        )
        assert result["valid"] is True

    def test_mysql_dialect(self):
        result = validate_sql(
            "SELECT * FROM orders LIMIT 10",
            dialect="mysql",
        )
        assert result["valid"] is True

    # ── Edge cases ──────────────────────────────────────────────────────

    def test_select_with_subquery(self):
        result = validate_sql(
            "SELECT * FROM (SELECT id FROM orders WHERE total > 100) sub"
        )
        assert result["valid"] is True

    def test_select_with_complex_expression(self):
        result = validate_sql(
            "SELECT CASE WHEN total > 100 THEN 'high' ELSE 'low' END AS category, "
            "COUNT(*) FROM orders GROUP BY category"
        )
        assert result["valid"] is True

    def test_pragma_rejected(self):
        result = validate_sql("PRAGMA table_info('orders')")
        assert result["valid"] is False
