"""Tests for AST guardrail validation."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from guard.ast_validator import validate_sql_ast


def test_valid_simple_select():
    ok, issues = validate_sql_ast("SELECT * FROM users WHERE id = 1")
    assert ok, f"Expected valid but got: {issues}"


def test_valid_select_with_columns():
    ok, issues = validate_sql_ast("SELECT id, name, email FROM users ORDER BY name")
    assert ok, f"Expected valid but got: {issues}"


def test_valid_with_cte():
    ok, issues = validate_sql_ast(
        "WITH active AS (SELECT * FROM users WHERE status = 1) "
        "SELECT * FROM active"
    )
    assert ok, f"Expected valid but got: {issues}"


def test_valid_join():
    ok, issues = validate_sql_ast(
        "SELECT u.name, o.total FROM users u "
        "JOIN orders o ON u.id = o.user_id "
        "WHERE o.total > 100 ORDER BY o.total DESC LIMIT 10"
    )
    assert ok, f"Expected valid but got: {issues}"


def test_valid_group_by():
    ok, issues = validate_sql_ast(
        "SELECT user_id, COUNT(*) AS cnt, SUM(amount) AS total "
        "FROM orders GROUP BY user_id HAVING COUNT(*) > 3"
    )
    assert ok, f"Expected valid but got: {issues}"


def test_syntax_error():
    # Missing table name after FROM — genuine parse error
    ok, issues = validate_sql_ast("SELECT * FROM WHERE x = 1")
    assert not ok
    assert any("parse" in i.get("detail", "").lower() for i in issues)


def test_forbidden_insert():
    ok, issues = validate_sql_ast("INSERT INTO users VALUES (1, 'test')")
    assert not ok
    assert any("forbidden" in i.get("type", "").lower() for i in issues)


def test_forbidden_delete():
    ok, issues = validate_sql_ast("DELETE FROM users WHERE id = 1")
    assert not ok


def test_forbidden_drop():
    ok, issues = validate_sql_ast("DROP TABLE users")
    assert not ok


def test_forbidden_update():
    ok, issues = validate_sql_ast("UPDATE users SET name = 'x' WHERE id = 1")
    assert not ok


def test_forbidden_create():
    ok, issues = validate_sql_ast("CREATE TABLE t (id int)")
    assert not ok


def test_empty_sql():
    ok, issues = validate_sql_ast("")
    assert not ok


def test_multi_statement_rejected():
    ok, issues = validate_sql_ast("SELECT * FROM users; SELECT * FROM orders")
    assert not ok
    assert any("multiple" in i.get("detail", "").lower() for i in issues)


if __name__ == "__main__":
    test_valid_simple_select()
    test_valid_select_with_columns()
    test_valid_with_cte()
    test_valid_join()
    test_valid_group_by()
    test_syntax_error()
    test_forbidden_insert()
    test_forbidden_delete()
    test_forbidden_drop()
    test_forbidden_update()
    test_forbidden_create()
    test_empty_sql()
    test_multi_statement_rejected()
    print("All AST tests passed!")
