"""SQL Executor — sandbox execution with safety gates."""
from sqlalchemy import text
from .config import Config
from .schema import get_engine

FORBIDDEN_KW = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "PRAGMA", "ATTACH", "DETACH"]


def _safety_check(sql: str) -> tuple[bool, str]:
    """Pre-execution safety gate. Returns (safe, reason)."""
    sql_upper = sql.upper()

    for kw in FORBIDDEN_KW:
        if kw in sql_upper:
            return False, f"Forbidden keyword in SQL: {kw}"

    statements = [s.strip() for s in sql.split(";") if s.strip()]
    if len(statements) > 1:
        return False, "Multiple statements detected"

    return True, ""


def execute_sql(sql: str, max_rows: int = 1000, database_url: str | None = None) -> dict:
    """
    Execute a SELECT SQL on the target database (read-only).

    Returns:
        {
            "success": bool,
            "data": list[tuple] | None,
            "columns": list[str] | None,
            "error": str | None,
            "row_count": int,
        }
    """
    safe, reason = _safety_check(sql)
    if not safe:
        return {"success": False, "data": None, "columns": None, "error": reason, "row_count": 0}

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
            "row_count": len(rows),
        }
    except Exception as e:
        return {
            "success": False,
            "data": None,
            "columns": None,
            "error": str(e),
            "row_count": 0,
        }
