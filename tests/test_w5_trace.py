"""Tests for trace ID generation and propagation."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_trace_logger_creates_id():
    from observability.logger import TraceLogger
    tlog = TraceLogger()
    assert tlog.trace_id
    assert len(tlog.trace_id) == 12


def test_trace_logger_respects_given_id():
    from observability.logger import TraceLogger
    tlog = TraceLogger("abc123")
    assert tlog.trace_id == "abc123"


def test_trace_logger_unique_ids():
    from observability.logger import TraceLogger
    ids = {TraceLogger().trace_id for _ in range(10)}
    assert len(ids) == 10, "All trace IDs should be unique"


def test_trace_logger_node_enter_exit():
    from observability.logger import TraceLogger
    tlog = TraceLogger("test123")
    tlog.node_enter("test_node", {"key": "val"})
    tlog.node_exit("test_node", {"result": "ok"})
    assert "test_node:enter" not in tlog._start_times  # cleared on exit


def test_trace_logger_guard_result():
    from observability.logger import TraceLogger
    tlog = TraceLogger("guard_test")
    tlog.guard_result(True, [{"type": "safety", "detail": "ok"}])
    # No exception = pass


def test_trace_logger_router_decision():
    from observability.logger import TraceLogger
    tlog = TraceLogger("router_test")
    tlog.router_decision("complex", 2, "heuristic")
    # No exception = pass


def test_trace_logger_sql_exec():
    from observability.logger import TraceLogger
    tlog = TraceLogger("sql_test")
    tlog.sql_exec(True, 42, 1.5, "")
    # No exception = pass


def test_trace_logger_llm_call():
    from observability.logger import TraceLogger
    tlog = TraceLogger("llm_test")
    tlog.llm_call("deepseek-chat", {"prompt_tokens": 1200, "completion_tokens": 300, "total_tokens": 1500}, 3.2)
    # No exception = pass


def test_trace_logger_semantic_verdict():
    from observability.logger import TraceLogger
    tlog = TraceLogger("sem_test")
    tlog.semantic_verdict(True, "")
    tlog.semantic_verdict(False, "SQL returns wrong aggregation")
    # No exception = pass


if __name__ == "__main__":
    test_trace_logger_creates_id()
    test_trace_logger_respects_given_id()
    test_trace_logger_unique_ids()
    test_trace_logger_node_enter_exit()
    test_trace_logger_guard_result()
    test_trace_logger_router_decision()
    test_trace_logger_sql_exec()
    test_trace_logger_llm_call()
    test_trace_logger_semantic_verdict()
    print("All Trace tests passed!")
