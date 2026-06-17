"""Executor node — shared SQL execution entry point."""
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from tools.sql_executor import execute_sql as _execute_sql
from agent.state import AgentState

EXEC_TIMEOUT_S = 60


def run_sql(sql: str, timeout_s: int = EXEC_TIMEOUT_S, database_url: str | None = None) -> dict:
    """Execute a single SQL in sandbox with timeout. Returns {success, data, columns, error, row_count}.

    Uses ThreadPoolExecutor for cross-platform timeout (Windows compatible).
    """
    t0 = time.time()
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        future = pool.submit(_execute_sql, sql, database_url=database_url)
        result = future.result(timeout=timeout_s)
        elapsed = time.time() - t0
        result["_elapsed_ms"] = round(elapsed * 1000)
        return result
    except FutureTimeoutError:
        elapsed = time.time() - t0
        return {
            "success": False,
            "error": f"Query timed out after {timeout_s}s",
            "data": None,
            "columns": None,
            "row_count": 0,
            "_elapsed_ms": round(elapsed * 1000),
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"Execution error: {str(e)[:300]}",
            "data": None,
            "columns": None,
            "row_count": 0,
        }
    finally:
        pool.shutdown(wait=False)  # don't block on stuck SQL thread


def executor_node(state: AgentState) -> dict:
    """Graph node wrapper — execute state.sql and record exec_result + attempt.

    Only used for standalone execution (not called within Voter flow).
    When Voter already ran, exec_result is pre-populated and this can be skipped.
    """
    t0 = time.time()
    sql = state.get("sql", "")
    exec_attempts = list(state.get("exec_attempts", []))
    retry_count = state.get("retry_count", 0)
    database_url = state.get("database_url")

    if not sql:
        exec_result = {
            "success": False, "error": "No SQL to execute",
            "data": None, "columns": None, "row_count": 0,
        }
        node_latency = dict(state.get("node_latency", {}))
        node_latency["executor"] = round(time.time() - t0, 3)
        return {"exec_result": exec_result, "exec_attempts": exec_attempts, "node_latency": node_latency}

    # Skip if already executed by Voter (same sql)
    existing = state.get("exec_result")
    if existing and existing.get("_sql") == sql:
        return {}

    tlog = state.get("tlog")
    if tlog:
        tlog.node_enter("executor", {"retry_count": retry_count})

    result = run_sql(sql, database_url=database_url)
    attempt_num = retry_count + 1
    exec_attempts.append({
        "attempt": attempt_num,
        "sql": sql,
        "success": result["success"],
        "error": result.get("error"),
        "row_count": result["row_count"],
    })

    if tlog:
        tlog.sql_exec(result["success"], result.get("row_count", 0),
                       result.get("_elapsed_ms", 0) / 1000, result.get("error", ""))
        tlog.node_exit("executor", {"success": result["success"], "row_count": result.get("row_count", 0)})

    node_latency = dict(state.get("node_latency", {}))
    node_latency["executor"] = round(time.time() - t0, 3)

    return {
        "exec_result": result,
        "exec_attempts": exec_attempts,
        "retry_count": attempt_num,
        "node_latency": node_latency,
    }
