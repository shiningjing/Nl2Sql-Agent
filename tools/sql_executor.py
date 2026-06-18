"""SQL Executor — sandbox execution with safety gates."""
from sqlalchemy import text
from storage.config import Config
from retrieval.schema import get_engine
from guard.safety_rules import check_safety


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
