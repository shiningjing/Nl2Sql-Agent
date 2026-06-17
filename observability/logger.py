"""Structured JSON-line logger with trace context.

Usage:
    from observability.logger import TraceLogger
    tlog = TraceLogger(trace_id)
    tlog.node_enter("generator", {"question_len": 42})
    tlog.node_exit("generator", {"sql_len": 150})
    tlog.llm_call("deepseek-chat", {"prompt_tokens": 1200, "completion_tokens": 300})
"""

import json
import logging
import os
import sys
import time
import uuid
from pathlib import Path

_log = logging.getLogger("nl2sql")


def init_logging(level: int = logging.INFO, log_file: str | None = None):
    """Configure root logger for structured JSON output."""
    handler = logging.StreamHandler(sys.stderr)
    if log_file:
        log_dir = Path(log_file).parent
        log_dir.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(log_file, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(message)s"))
    _log.handlers.clear()
    _log.addHandler(handler)
    _log.setLevel(level)


class TraceLogger:
    def __init__(self, trace_id: str | None = None, log_dir: str | None = None):
        self.trace_id = trace_id or uuid.uuid4().hex[:12]
        self._start_times: dict[str, float] = {}
        self.events: list[dict] = []  # in-memory buffer for post-hoc extraction
        self._stream_file: str = ""  # path to streaming JSONL file
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
            self._stream_file = os.path.join(log_dir, f"{self.trace_id}.jsonl")

    def _emit(self, event: str, data: dict):
        record = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
            "trace_id": self.trace_id,
            "event": event,
            **data,
        }
        self.events.append(record)
        _log.info(json.dumps(record, ensure_ascii=False, default=str))
        # Streaming write: each event flushed to disk immediately
        if self._stream_file:
            try:
                with open(self._stream_file, "a", encoding="utf-8") as f:
                    f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
                    f.flush()
                    os.fsync(f.fileno())
            except Exception:
                pass  # never let trace I/O break the pipeline

    def node_enter(self, node: str, meta: dict | None = None,
                   model: str = "", prompt_version: str = ""):
        key = f"{node}:enter"
        self._start_times[key] = time.time()
        data: dict = {"node": node, **(meta or {})}
        if model:
            data["model"] = model
        if prompt_version:
            data["prompt_version"] = prompt_version
        self._emit("node_enter", data)

    def node_exit(self, node: str, meta: dict | None = None,
                  status: str = "success", model: str = "", prompt_version: str = ""):
        key = f"{node}:enter"
        duration = 0.0
        if key in self._start_times:
            duration = round(time.time() - self._start_times.pop(key), 3)
        data: dict = {"node": node, "duration_s": duration, "status": status, **(meta or {})}
        if model:
            data["model"] = model
        if prompt_version:
            data["prompt_version"] = prompt_version
        self._emit("node_exit", data)

    def node_error(self, node: str, error_type: str, error_msg: str):
        key = f"{node}:enter"
        duration = 0.0
        if key in self._start_times:
            duration = round(time.time() - self._start_times.pop(key), 3)
        self._emit("node_error", {
            "node": node,
            "duration_s": duration,
            "error_type": error_type,
            "error": error_msg[:300],
        })

    def llm_call(self, model: str, usage: dict, duration_s: float = 0, node: str = ""):
        data: dict = {
            "model": model,
            "prompt_tokens": usage.get("prompt_tokens", 0),
            "completion_tokens": usage.get("completion_tokens", 0),
            "total_tokens": usage.get("total_tokens", 0),
            "duration_s": round(duration_s, 3),
        }
        if node:
            data["node"] = node
        self._emit("llm_call", data)

    def llm_error(self, model: str, error_type: str, error_msg: str, duration_s: float = 0, node: str = ""):
        data: dict = {
            "model": model,
            "error_type": error_type,
            "error": error_msg[:300],
            "duration_s": round(duration_s, 3),
        }
        if node:
            data["node"] = node
        self._emit("llm_error", data)

    def sql_exec(self, success: bool, row_count: int, duration_s: float, error: str = ""):
        self._emit("sql_exec", {
            "success": success,
            "row_count": row_count,
            "duration_s": round(duration_s, 3),
            "error": error[:120] if error else "",
        })

    def guard_result(self, passed: bool, issues: list[dict]):
        self._emit("guard", {
            "passed": passed,
            "issue_count": len(issues),
            "issue_types": [i.get("type", "?") for i in issues],
        })

    def router_decision(self, complexity: str, score: int, method: str):
        self._emit("router", {
            "complexity": complexity,
            "score": score,
            "method": method,
        })

    def semantic_verdict(self, passed: bool, reason: str):
        self._emit("semantic_check", {
            "passed": passed,
            "reason": reason[:200] if reason else "",
        })

    def get_node_timings(self) -> dict[str, float]:
        """Aggregate node_exit events into {node: total_duration_s}."""
        timings: dict[str, float] = {}
        for e in self.events:
            if e["event"] == "node_exit":
                node = e.get("node", "?")
                dur = e.get("duration_s", 0)
                timings[node] = round(timings.get(node, 0) + dur, 3)
        return timings

    def export(self, path: str) -> str:
        """Write all events to a JSON file. Returns the absolute path written."""
        import os
        abs_path = os.path.abspath(path)
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        with open(abs_path, "w", encoding="utf-8") as f:
            json.dump({"trace_id": self.trace_id, "events": self.events}, f, ensure_ascii=False, default=str, indent=2)
        return abs_path

    # ── API-level events ────────────────────────────────────────────────────

    def api_request(self, phase: str, method: str, path: str,
                    status: int = 0, elapsed_ms: float = 0, client_ip: str = ""):
        self._emit(f"api_request_{phase}", {
            "method": method,
            "path": path,
            "status": status,
            "elapsed_ms": round(elapsed_ms, 1),
            "client_ip": client_ip,
        })

    def cache_hit(self, similarity: float, cached_question: str):
        self._emit("cache_hit", {
            "similarity": round(similarity, 4),
            "cached_question": cached_question[:100],
        })

    def cache_miss(self, reason: str):
        self._emit("cache_miss", {"reason": reason})

    def cache_store(self, question: str):
        self._emit("cache_store", {"question": question[:100]})


# ── Log cleanup ──────────────────────────────────────────────────────────────

def cleanup_trace_dirs(base_dir: str, keep: int = 5) -> int:
    """Remove oldest trace directories, keeping the `keep` most recent.
    Returns number of directories removed."""
    if not os.path.isdir(base_dir):
        return 0
    try:
        entries = []
        for name in os.listdir(base_dir):
            full = os.path.join(base_dir, name)
            if os.path.isdir(full) and name.replace("_", "").replace("-", "").isdigit():
                entries.append((name, os.path.getmtime(full), full))
        entries.sort(key=lambda e: e[1])  # oldest first
        removed = 0
        while len(entries) > keep:
            name, _, full = entries.pop(0)
            import shutil
            shutil.rmtree(full, ignore_errors=True)
            removed += 1
    except Exception:
        return 0
    return removed
