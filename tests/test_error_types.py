"""Tests for guard/error_types.py and guard/error_classifier.py."""

import pytest
from guard.error_types import ErrorType
from guard.error_classifier import classify_exec_error


class TestErrorType:
    """Enum values and JSON serialization."""

    def test_all_values_are_strings(self):
        for e in ErrorType:
            assert isinstance(e.value, str)

    def test_json_serializable(self):
        import json
        d = {"error_type": ErrorType.TIMEOUT}
        s = json.dumps(d)
        assert "timeout" in s

    def test_str_enum_equality(self):
        assert ErrorType.TIMEOUT == "timeout"
        assert ErrorType.MISSING_COLUMN == "missing_column"


class TestClassifyExecError:

    def test_missing_column_sqlite(self):
        ty, hint = classify_exec_error("no such column: student_name")
        assert ty == ErrorType.MISSING_COLUMN
        assert "student_name" in hint

    def test_missing_table_sqlite(self):
        ty, hint = classify_exec_error("no such table: orders")
        assert ty == ErrorType.MISSING_TABLE
        assert "orders" in hint

    def test_ambiguous_column(self):
        ty, hint = classify_exec_error("ambiguous column name: id")
        assert ty == ErrorType.AMBIGUOUS_COLUMN

    def test_bad_function(self):
        ty, hint = classify_exec_error("no such function: foobar")
        assert ty == ErrorType.BAD_FUNCTION

    def test_syntax_error(self):
        ty, hint = classify_exec_error("syntax error near 'WHERE'")
        assert ty == ErrorType.SYNTAX_ERROR

    def test_group_by_error(self):
        ty, hint = classify_exec_error("GROUP BY error: column must appear in GROUP BY")
        assert ty == ErrorType.GROUP_BY_ERROR

    def test_unknown_fallback(self):
        ty, hint = classify_exec_error("Some completely unexpected database error")
        assert ty == ErrorType.EXECUTION_ERROR

    def test_timeout(self):
        """Timeout is handled at executor/MCP layer, not classifier, but verify no crash."""
        ty, hint = classify_exec_error("query timed out after 30000ms")
        assert ty == ErrorType.EXECUTION_ERROR  # falls through to unknown
