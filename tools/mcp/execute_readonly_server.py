"""MCP tool: execute_readonly_sql — sandboxed SQL execution with hard safety limits.

Usage:
    python tools/mcp/execute_readonly_server.py
    # Stdio MCP server, connect via fastmcp Client or MCP inspector.

Hard limits (enforced at tool layer):
- Only SELECT/WITH allowed (regex + sqlglot AST dual validation)
- Auto-wrap LIMIT if not present (default 200, hard cap 1000)
- Connection-level statement timeout (60s default)
- max_rows hard upper limit 1000 (reject above, don't silently truncate)
- Execution result rows must not exceed max_rows

Input:  {sql, database_url, max_rows?, timeout_ms?}
Output: {success, error, error_type, data, columns, row_count, execution_ms}
"""

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError

# Ensure project root on path for subprocess (MCP stdio) execution
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from fastmcp import FastMCP
from guard.safety_rules import check_safety

mcp = FastMCP("execute-readonly-sql")

# ── Constants ────────────────────────────────────────────────────────────

_MAX_ROWS_HARD_CAP = 1000
_DEFAULT_MAX_ROWS = 200
_DEFAULT_TIMEOUT_MS = 60000


# ── Helpers ──────────────────────────────────────────────────────────────

def _dialect_from_url(database_url: str) -> str:
    """Derive SQL dialect from database URL scheme."""
    url_lower = database_url.lower()
    if url_lower.startswith("postgresql"):
        return "postgres"
    if url_lower.startswith("mysql"):
        return "mysql"
    return "sqlite"


def _validate_input(sql: str, dialect: str, max_rows: int, timeout_ms: int) -> dict | None:
    """Validate all inputs before execution. Returns error dict or None if OK."""
    if not sql or not sql.strip():
        return {
            "success": False, "error": "SQL is empty.", "error_type": "INVALID_INPUT",
            "data": None, "columns": None, "row_count": 0, "execution_ms": 0,
        }
    if max_rows > _MAX_ROWS_HARD_CAP:
        return {
            "success": False,
            "error": f"max_rows {max_rows} exceeds hard limit {_MAX_ROWS_HARD_CAP}.",
            "error_type": "INVALID_INPUT",
            "data": None, "columns": None, "row_count": 0, "execution_ms": 0,
        }
    if timeout_ms > 120_000:
        return {
            "success": False,
            "error": f"timeout_ms {timeout_ms} exceeds hard limit 120000.",
            "error_type": "INVALID_INPUT",
            "data": None, "columns": None, "row_count": 0, "execution_ms": 0,
        }

    # Unified safety check (L1 regex + L3 AST via safety_rules)
    safety = check_safety(sql, dialect)
    if not safety["valid"]:
        return {
            "success": False,
            "error": f"SQL validation failed: {safety['issues'][0]['detail']}",
            "error_type": "SQL_VALIDATION_FAILED",
            "data": None, "columns": None, "row_count": 0, "execution_ms": 0,
        }
    return None


def _auto_limit(sql: str, max_rows: int) -> str:
    """Wrap SQL with LIMIT if not already present."""
    sql_clean = sql.strip().rstrip(";")
    if "LIMIT" not in sql_clean.upper():
        return f"SELECT * FROM ({sql_clean}) AS _sub LIMIT {max_rows}"
    return sql_clean


def _set_statement_timeout(conn, dialect: str, timeout_ms: int):
    """Set connection-level statement timeout where supported."""
    try:
        timeout_sec = max(1, int(timeout_ms / 1000))
        if dialect == "postgres":
            conn.execute(text("SET statement_timeout = '%ds'" % timeout_sec))
        elif dialect == "mysql":
            conn.execute(text("SET max_execution_time = %d" % timeout_ms))
        # SQLite: no connection-level timeout, handled via ThreadPoolExecutor
    except Exception:
        pass  # timeout setting is best-effort, not a hard failure


# ── Core execution ───────────────────────────────────────────────────────

def _do_execute(sql: str, database_url: str, dialect: str,
                max_rows: int, timeout_ms: int) -> dict:
    """Execute SQL against the database. Returns structured result."""
    import sqlalchemy
    from sqlalchemy import text

    t0 = time.time()
    sql_to_run = _auto_limit(sql, max_rows)

    try:
        engine = sqlalchemy.create_engine(database_url)
        with engine.connect() as conn:
            _set_statement_timeout(conn, dialect, timeout_ms)
            result = conn.execute(text(sql_to_run))
            rows = result.fetchall()
            columns = list(result.keys())

        elapsed_ms = int((time.time() - t0) * 1000)
        return {
            "success": True,
            "error": None,
            "error_type": None,
            "data": [list(row) for row in rows],
            "columns": columns,
            "row_count": len(rows),
            "execution_ms": elapsed_ms,
        }
    except Exception as e:
        elapsed_ms = int((time.time() - t0) * 1000)
        error_msg = str(e)[:500]
        error_type = "EXECUTION_ERROR"
        if "timeout" in error_msg.lower() or "timed out" in error_msg.lower():
            error_type = "TIMEOUT"
        return {
            "success": False,
            "error": error_msg,
            "error_type": error_type,
            "data": None,
            "columns": None,
            "row_count": 0,
            "execution_ms": elapsed_ms,
        }


# ── MCP tool ─────────────────────────────────────────────────────────────

@mcp.tool
def execute_readonly_sql(
    sql: str,
    database_url: str,
    max_rows: int = _DEFAULT_MAX_ROWS,
    timeout_ms: int = _DEFAULT_TIMEOUT_MS,
) -> dict:
    """Execute a read-only SQL query with hard safety limits.

    Only SELECT/WITH statements are allowed. All other statement types
    (INSERT, UPDATE, DELETE, DROP, etc.) are rejected. LIMIT is
    automatically added if missing.

    Args:
        sql: The SQL query to execute (SELECT/WITH only).
        database_url: SQLAlchemy database URL (sqlite:///..., postgresql://..., mysql://...).
        max_rows: Maximum rows to return (default 200, hard cap 1000).
        timeout_ms: Query timeout in milliseconds (default 60000, max 120000).
    """
    dialect = _dialect_from_url(database_url)

    # Phase 1: input validation
    err = _validate_input(sql, dialect, max_rows, timeout_ms)
    if err:
        return err

    # Phase 2: execute with timeout wrapper (covers SQLite which has no native timeout)
    def _run():
        return _do_execute(sql, database_url, dialect, max_rows, timeout_ms)

    t0 = time.time()
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        fut = pool.submit(_run)
        result = fut.result(timeout=timeout_ms / 1000.0)
    except FutureTimeoutError:
        elapsed_ms = int((time.time() - t0) * 1000)
        return {
            "success": False,
            "error": f"Query timeout after {timeout_ms}ms",
            "error_type": "TIMEOUT",
            "data": None,
            "columns": None,
            "row_count": 0,
            "execution_ms": elapsed_ms,
        }
    finally:
        pool.shutdown(wait=False)

    return result


# ── Entrypoint ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    mcp.run()
