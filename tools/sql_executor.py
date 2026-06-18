"""SQL Executor — sandbox execution with safety gates."""
from sqlalchemy import text
from storage.config import Config
from retrieval.schema import get_engine
from guard.safety_rules import check_safety
from guard.error_types import ErrorType


def execute_sql(sql: str, max_rows: int = 1000, database_url: str | None = None) -> dict:
    """
    Execute a SELECT SQL on the target database (read-only).

    Returns:
        {
            "success": bool,
            "data": list[tuple] | None,
            "columns": list[str] | None,
            "error": str | None,
            "error_type": str | None,
            "row_count": int,
        }
    """
    # Unified safety check (L1 regex + L3 AST)
    dialect_map = {"postgresql": "postgres", "mysql": "mysql"}
    dialect = "sqlite"
    if database_url:
        for prefix, d in dialect_map.items():
            if database_url.startswith(prefix):
                dialect = d
                break
    safety = check_safety(sql, dialect)
    if not safety["valid"]:
        reason = safety["issues"][0]["detail"] if safety["issues"] else "Unknown validation error"
        return {
            "success": False, "data": None, "columns": None,
            "error": reason, "error_type": ErrorType.SQL_VALIDATION, "row_count": 0,
        }

    try:
        engine = get_engine(database_url)
        with engine.connect() as conn:
            # Wrap with LIMIT if not present, to enforce row cap
            sql_to_run = sql.strip().rstrip(";")
            if "LIMIT" not in sql_to_run.upper():
                sql_to_run = f"SELECT * FROM ({sql_to_run}) AS _sub LIMIT {max_rows}"

            result = conn.execute(text(sql_to_run))
            rows = result.fetchall()
            columns = list(result.keys())

        return {
            "success": True,
            "data": rows,
            "columns": columns,
            "error": None,
            "error_type": None,
            "row_count": len(rows),
        }
    except Exception as e:
        error_msg = str(e)
        if "timeout" in error_msg.lower() or "timed out" in error_msg.lower():
            err_type = ErrorType.TIMEOUT
        else:
            from guard.error_classifier import classify_exec_error
            err_type, _ = classify_exec_error(error_msg)
        return {
            "success": False,
            "data": None,
            "columns": None,
            "error": error_msg,
            "error_type": str(err_type),
            "row_count": 0,
        }
