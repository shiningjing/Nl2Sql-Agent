"""Standard error codes for SQL validation and execution."""

from enum import StrEnum


class ErrorType(StrEnum):
    # Input validation
    EMPTY_SQL = "empty_sql"
    SQL_VALIDATION = "sql_validation"
    INVALID_INPUT = "invalid_input"

    # Safety
    PERMISSION_DENIED = "permission_denied"

    # Schema cross-reference
    MISSING_COLUMN = "missing_column"
    MISSING_TABLE = "missing_table"
    AMBIGUOUS_COLUMN = "ambiguous_column"

    # Execution
    SYNTAX_ERROR = "syntax_error"
    BAD_FUNCTION = "bad_function"
    GROUP_BY_ERROR = "group_by_error"
    TIMEOUT = "timeout"
    EXECUTION_ERROR = "execution_error"
    SEMANTIC_MISMATCH = "semantic_mismatch"
