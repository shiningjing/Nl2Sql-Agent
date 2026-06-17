"""Eval task manager — background execution with Redis progress tracking.

Lifecycle: PENDING -> RUNNING -> COMPLETED / FAILED / CANCELLED

Concurrency: one task runs at a time. Within a task, samples are parallelized
via ThreadPoolExecutor (default 4 workers) since each sample is an independent
LLM call + SQL execution.

Progress is persisted to Redis (survives API restart). In-memory cancel
events are lost on restart — the orphaned thread keeps running but can't
be cancelled (acceptable tradeoff; restart the process to kill it).
"""
import json
import logging
import os
import sys
import threading
import time
import traceback
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from guard.error_classifier import run
from storage.config import Config
from agent.graphs.full_graph import create_full_graph
from evaluation.metrics import exec_match, ves_score, load_gold
from storage.redis_cache import get_redis

_log = logging.getLogger("nl2sql.eval")

# ── Constants ──────────────────────────────────────────────────────────────────

STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
STATUS_CANCELLED = "cancelled"

_TASK_PREFIX = "eval:task"
_TASKS_SET = "eval:tasks"
_TASK_TTL = 86400 * 7  # 7 days

# In-memory cancel events (lost on restart; orphaned threads keep running)
_active_tasks: dict[str, threading.Event] = {}


def _redis():
    return get_redis()


# ── Progress data class ────────────────────────────────────────────────────────

class TaskProgress:
    __slots__ = (
        "task_id", "status", "experiment", "configs",
        "current_config", "current_sample", "total_configs", "total_samples",
        "overall_completed", "overall_passed",
        "created_at", "started_at", "completed_at",
        "report_dir", "error", "database_url",
    )

    def __init__(
        self,
        task_id: str = "",
        status: str = STATUS_PENDING,
        experiment: str = "",
        configs: list[dict] | None = None,
        current_config: str = "",
        current_sample: int = 0,
        total_configs: int = 0,
        total_samples: int = 0,
        overall_completed: int = 0,
        overall_passed: int = 0,
        created_at: str = "",
        started_at: str = "",
        completed_at: str = "",
        report_dir: str = "",
        error: str = "",
        database_url: str = "",
    ):
        self.task_id = task_id
        self.status = status
        self.experiment = experiment
        self.configs = configs or []
        self.current_config = current_config
        self.current_sample = current_sample
        self.total_configs = total_configs
        self.total_samples = total_samples
        self.overall_completed = overall_completed
        self.overall_passed = overall_passed
        self.created_at = created_at
        self.started_at = started_at
        self.completed_at = completed_at
        self.report_dir = report_dir
        self.error = error
        self.database_url = database_url

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "status": self.status,
            "experiment": self.experiment,
            "configs": self.configs,
            "current_config": self.current_config,
            "current_sample": self.current_sample,
            "total_configs": self.total_configs,
            "total_samples": self.total_samples,
            "overall_completed": self.overall_completed,
            "overall_passed": self.overall_passed,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "report_dir": self.report_dir,
            "error": self.error,
            "database_url": self.database_url,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "TaskProgress":
        return cls(
            task_id=d.get("task_id", ""),
            status=d.get("status", STATUS_PENDING),
            experiment=d.get("experiment", ""),
            configs=d.get("configs", []),
            current_config=d.get("current_config", ""),
            current_sample=d.get("current_sample", 0),
            total_configs=d.get("total_configs", 0),
            total_samples=d.get("total_samples", 0),
            overall_completed=d.get("overall_completed", 0),
            overall_passed=d.get("overall_passed", 0),
            created_at=d.get("created_at", ""),
            started_at=d.get("started_at", ""),
            completed_at=d.get("completed_at", ""),
            report_dir=d.get("report_dir", ""),
            error=d.get("error", ""),
            database_url=d.get("database_url", ""),
        )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Redis persistence ──────────────────────────────────────────────────────────

def _save_task(p: TaskProgress) -> None:
    r = _redis()
    if r is None:
        return
    r.setex(
        f"{_TASK_PREFIX}:{p.task_id}",
        _TASK_TTL,
        json.dumps(p.to_dict(), ensure_ascii=False),
    )
    r.sadd(_TASKS_SET, p.task_id)


def _load_task(task_id: str) -> TaskProgress | None:
    r = _redis()
    if r is None:
        return None
    raw = r.get(f"{_TASK_PREFIX}:{task_id}")
    if raw is None:
        return None
    try:
        return TaskProgress.from_dict(json.loads(raw))
    except json.JSONDecodeError:
        return None


# ── Public API ─────────────────────────────────────────────────────────────────

def get_task_status(task_id: str) -> TaskProgress | None:
    """Read current task state from Redis."""
    return _load_task(task_id)


def list_tasks() -> list[dict]:
    """List all tasks (latest first), returning lightweight summaries."""
    r = _redis()
    if r is None:
        return []
    ids = r.smembers(_TASKS_SET)
    tasks = []
    for tid in ids:
        raw = r.get(f"{_TASK_PREFIX}:{tid}")
        if raw is None:
            r.srem(_TASKS_SET, tid)
            continue
        try:
            d = json.loads(raw)
            tasks.append({
                "task_id": d.get("task_id"),
                "status": d.get("status"),
                "experiment": d.get("experiment"),
                "overall_completed": d.get("overall_completed", 0),
                "total_samples": d.get("total_samples", 0),
                "overall_passed": d.get("overall_passed", 0),
                "created_at": d.get("created_at"),
                "completed_at": d.get("completed_at"),
            })
        except json.JSONDecodeError:
            r.srem(_TASKS_SET, tid)
    tasks.sort(key=lambda t: t["created_at"], reverse=True)
    return tasks


def is_task_running() -> bool:
    """Check if any task is currently running."""
    for tid, evt in list(_active_tasks.items()):
        if not evt.is_set():
            return True
    return False


def cancel_task(task_id: str) -> bool:
    """Signal cancellation for a running task. Returns True if the task was found."""
    evt = _active_tasks.get(task_id)
    if evt is None:
        return False
    evt.set()
    return True


def start_eval_task(
    gold_path: str,
    configs: list[dict],
    experiment: str = "ablation",
    database_url: str | None = None,
    output_dir: str | None = None,
    max_workers: int = 4,
) -> str:
    """Start a background evaluation. Returns task_id.

    Args:
        gold_path: path to gold.jsonl
        configs: list of config dicts, each with name + pipeline flags
        experiment: label for this experiment (e.g. "R0-R6", "knowledge_ablation")
        database_url: override database URL for all samples
        output_dir: where to write the report (default: reports/)
        max_workers: ThreadPoolExecutor concurrency per config (default 4)

    Returns task_id. Raises RuntimeError if a task is already running.
    """
    if is_task_running():
        # Return the existing running task id instead of failing
        for tid in _active_tasks:
            if tid not in [t for t in _active_tasks if _active_tasks[t].is_set()]:
                return tid
        # Shouldn't reach here, but fallback
        raise RuntimeError("An evaluation task is already running")

    task_id = str(uuid.uuid4())
    default_output = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "reports",
    )

    # Pre-compute totals for accurate progress
    cases = load_gold(gold_path)
    n_cases = len(cases)
    config_entries = []
    for cfg in configs:
        config_entries.append({
            "name": cfg["name"],
            "total": n_cases,
            "completed": 0,
            "passed": 0,
        })

    p = TaskProgress(
        task_id=task_id,
        status=STATUS_PENDING,
        experiment=experiment,
        configs=config_entries,
        total_configs=len(configs),
        total_samples=n_cases * len(configs),
        created_at=_now(),
        database_url=database_url or Config.DATABASE_URL,
    )
    _save_task(p)

    cancel_evt = threading.Event()
    _active_tasks[task_id] = cancel_evt

    thread = threading.Thread(
        target=_run_eval_thread,
        args=(task_id, gold_path, configs, database_url, output_dir or default_output, cancel_evt, max_workers),
        daemon=True,
    )
    thread.start()

    _log.info(json.dumps({"event": "eval_task_start", "task_id": task_id,
                          "experiment": experiment, "configs": len(configs),
                          "samples": n_cases}))
    return task_id


# ── Background execution ──────────────────────────────────────────────────────

def _run_eval_thread(
    task_id: str,
    gold_path: str,
    configs: list[dict],
    database_url: str | None,
    output_dir: str,
    cancel_evt: threading.Event,
    max_workers: int = 4,
) -> None:
    """Run eval in background, parallelizing samples within each config via ThreadPoolExecutor."""
    try:
        p = _load_task(task_id)
        if p is None:
            return
        p.status = STATUS_RUNNING
        p.started_at = _now()
        _save_task(p)

        cases = load_gold(gold_path)
        n_cases = len(cases)
        all_results = {}

        for cfg_idx, cfg in enumerate(configs):
            if cancel_evt.is_set():
                _finish_task(task_id, STATUS_CANCELLED, output_dir)
                return

            cfg_name = cfg["name"]
            _log.info(json.dumps({"event": "eval_config_start", "task_id": task_id,
                                  "config": cfg_name, "index": cfg_idx + 1,
                                  "total": len(configs), "max_workers": max_workers}))

            lock = threading.Lock()
            case_results: list[dict] = []
            passed = 0
            completed = 0
            total_time = 0.0
            total_ves = 0.0
            total_tokens = {"prompt": 0, "completion": 0, "total": 0}
            diff_stats: dict = defaultdict(lambda: {"passed": 0, "total": 0, "ves_sum": 0.0,
                                                    "tokens": 0, "time": 0.0})

            def _update_progress() -> None:
                """Snapshot counters → Redis (caller must hold lock)."""
                sp = _load_task(task_id)
                if sp is None:
                    return
                sp.current_config = cfg_name
                sp.current_sample = completed
                sp.overall_completed = cfg_idx * n_cases + completed
                sp.overall_passed = sum(
                    c.get("passed", 0) for c in sp.configs[:cfg_idx]
                ) + passed
                if cfg_idx < len(sp.configs):
                    sp.configs[cfg_idx]["completed"] = completed
                    sp.configs[cfg_idx]["passed"] = passed
                _save_task(sp)

            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {}
                for case in cases:
                    if cancel_evt.is_set():
                        break
                    futures[pool.submit(_run_one_sample, case, cfg, database_url)] = case

                for future in as_completed(futures):
                    if cancel_evt.is_set():
                        for f in futures:
                            f.cancel()
                        _finish_task(task_id, STATUS_CANCELLED, output_dir)
                        return

                    sample_result = future.result()
                    case = futures[future]

                    with lock:
                        elapsed = sample_result["time_s"]
                        total_time += elapsed
                        tu = sample_result.get("token_usage", {})
                        if tu:
                            for k in total_tokens:
                                total_tokens[k] += tu.get(k, 0)

                        ex = sample_result["ex"]
                        v = sample_result["ves"]
                        total_ves += v
                        difficulty = case.get("difficulty", "?")

                        if ex:
                            passed += 1
                            diff_stats[difficulty]["passed"] += 1
                            diff_stats[difficulty]["ves_sum"] += v
                        diff_stats[difficulty]["total"] += 1
                        diff_stats[difficulty]["tokens"] += tu.get("total", 0)
                        diff_stats[difficulty]["time"] += elapsed

                        case_results.append(sample_result)
                        completed += 1
                        _update_progress()

            # Sort by original order for report consistency
            case_results.sort(key=lambda c: c["id"])

            # ── Config summary ──
            diff_summary = {}
            for d in ["simple", "moderate", "challenging"]:
                ds = diff_stats[d]
                if ds["total"] > 0:
                    diff_summary[d] = {
                        "total": ds["total"],
                        "passed": ds["passed"],
                        "ex": ds["passed"] / ds["total"],
                        "avg_ves": ds["ves_sum"] / ds["total"],
                        "avg_tokens": round(ds["tokens"] / ds["total"]),
                        "avg_time": round(ds["time"] / ds["total"], 1),
                    }

            all_results[cfg_name] = {
                "config": cfg,
                "case_results": case_results,
                "passed": passed,
                "total": n_cases,
                "ex_rate": passed / n_cases if n_cases else 0,
                "avg_ves": round(total_ves / n_cases, 4) if n_cases else 0,
                "avg_time_s": round(total_time / n_cases, 1) if n_cases else 0,
                "total_tokens": total_tokens,
                "avg_tokens": round(total_tokens["total"] / n_cases) if n_cases else 0,
                "diff_summary": diff_summary,
            }

        # ── Generate report ──
        report_path = _write_report(all_results, output_dir, task_id)

        p = _load_task(task_id)
        if p:
            p.status = STATUS_COMPLETED
            p.completed_at = _now()
            p.report_dir = report_path
            p.overall_passed = sum(r["passed"] for r in all_results.values())
            _save_task(p)

        _log.info(json.dumps({"event": "eval_task_complete", "task_id": task_id,
                              "report": report_path}))

    except Exception:
        _log.error(json.dumps({"event": "eval_task_failed", "task_id": task_id,
                               "traceback": traceback.format_exc()}))
        p = _load_task(task_id)
        if p:
            p.status = STATUS_FAILED
            p.error = traceback.format_exc()[-500:]
            _save_task(p)
    finally:
        _active_tasks.pop(task_id, None)


def _run_one_sample(case: dict, cfg: dict, database_url: str | None) -> dict:
    """Run one pipeline config against one gold sample. Returns {id, ex, ves, ...}."""
    qid = case["id"]
    question = case["question"]
    gold_sql = case["gold_sql"]
    difficulty = case.get("difficulty", "?")

    t0 = time.time()

    use_full = cfg.get("use_full_graph", False)
    if use_full:
        graph = create_full_graph()
        initial_state = {
            "question": question,
            "rag_schema": cfg["rag_schema"],
            "rag_domain": cfg["rag_domain"],
            "multi_candidate": cfg.get("multi_candidate", True),
            "rag_k": cfg.get("k", 8),
            "rag_column_prune": cfg.get("rag_column_prune", False),
            "fewshot_enabled": cfg.get("fewshot_enabled", False),
            "database_url": database_url,
        }
        result_state = graph.invoke(initial_state)
        gen_sql = result_state.get("sql", "")
        exec_result = result_state.get("exec_result")
        exec_attempts = result_state.get("exec_attempts", [])
        review_rounds = result_state.get("review_rounds", [])
        tu = result_state.get("token_usage", {})
    else:
        result = run(
            question,
            rag_schema=cfg["rag_schema"],
            rag_domain=cfg["rag_domain"],
            reviewer_on=cfg["reviewer_on"],
            k=cfg.get("k", 8),
            database_url=database_url,
        )
        gen_sql = result.get("sql", "")
        exec_result = result.get("exec_result")
        exec_attempts = result.get("exec_attempts", [])
        review_rounds = result.get("review_rounds", [])
        tu = result.get("token_usage", {})

    elapsed = time.time() - t0

    # Compare with gold (with per-sample database_url)
    if gen_sql and exec_result and exec_result["success"]:
        match_info = exec_match(gold_sql, gen_sql, database_url=database_url)
    elif not gen_sql:
        match_info = {"ex": False, "detail": "SQL generation failed",
                      "gold_time_ms": 0, "gen_time_ms": 0, "gold_rows": 0, "gen_rows": 0}
    elif exec_result:
        match_info = {"ex": False, "detail": f"exec failed: {exec_result.get('error', '?')[:80]}",
                      "gold_time_ms": 0, "gen_time_ms": 0, "gold_rows": 0, "gen_rows": 0}
    else:
        match_info = {"ex": False, "detail": "no exec result",
                      "gold_time_ms": 0, "gen_time_ms": 0, "gold_rows": 0, "gen_rows": 0}

    ex = match_info["ex"]
    v = ves_score(ex, match_info["gold_time_ms"], match_info["gen_time_ms"])

    # ── RAG recall ──
    from evaluation.metrics import compute_rag_recall
    if use_full:
        _rag_chunks = result_state.get("rag_chunks", [])
    else:
        _rag_chunks = result.get("rag_chunks", [])
    rag_recall_info = compute_rag_recall(_rag_chunks, gold_sql)

    return {
        "id": qid,
        "question": question[:60],
        "type": case.get("type", "?"),
        "difficulty": difficulty,
        "ex": ex,
        "ves": round(v, 4),
        "detail": match_info["detail"],
        "gen_sql": gen_sql[:200] if gen_sql else "",
        "gold_sql": gold_sql[:200],
        "exec_attempts": len(exec_attempts),
        "review_rounds": len(review_rounds),
        "time_s": round(elapsed, 1),
        "tokens": tu.get("total", 0),
        "token_usage": tu,
        "gold_time_ms": round(match_info["gold_time_ms"], 2),
        "gen_time_ms": round(match_info["gen_time_ms"], 2),
        "rag_recall": rag_recall_info["recall"] if rag_recall_info else None,
        "rag_recall_detail": rag_recall_info,
    }


def _finish_task(task_id: str, status: str, output_dir: str) -> None:
    """Write final status for cancelled/failed tasks."""
    p = _load_task(task_id)
    if p is None:
        return
    p.status = status
    p.completed_at = _now()
    if p.overall_completed > 0 and status == STATUS_CANCELLED:
        # Generate partial report
        _log.info(f"Task {task_id} cancelled after {p.overall_completed} samples")
    _save_task(p)


def _write_report(results: dict, output_dir: str, task_id: str) -> str:
    """Write evaluation report as Markdown, return report path."""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(output_dir, f"eval_{task_id[:8]}_{timestamp}.md")

    config_names = list(results.keys())
    if not config_names:
        return path

    with open(path, "w", encoding="utf-8") as f:
        f.write(f"# NL2SQL Agent — Evaluation Report\n\n")
        f.write(f"**Task**: {task_id}\n")
        f.write(f"**Date**: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"**Model**: {Config.LLM_CHAT_MODEL}\n")
        f.write(f"**Test cases**: {results[config_names[0]]['total']}\n\n")

        # ── Overall ──
        f.write("## Overall\n\n")
        f.write("| Config | EX | VES | Passed | Avg Time | Avg Tokens | Total Tokens |\n")
        f.write("|--------|-----|-----|--------|----------|------------|-------------|\n")
        for name in config_names:
            r = results[name]
            f.write(f"| {name} | {r['ex_rate']:.1%} | {r['avg_ves']:.4f} | "
                    f"{r['passed']}/{r['total']} | {r['avg_time_s']}s | "
                    f"{r['avg_tokens']} | {r['total_tokens']['total']} |\n")

        # ── Per-difficulty ──
        f.write("\n## EX by Difficulty\n\n")
        f.write("| Config | Simple | Moderate | Challenging |\n")
        f.write("|--------|--------|----------|-------------|\n")
        for name in config_names:
            r = results[name]
            ds = r.get("diff_summary", {})
            parts = []
            for d in ["simple", "moderate", "challenging"]:
                if d in ds:
                    dd = ds[d]
                    parts.append(f"{dd['ex']:.1%} ({dd['passed']}/{dd['total']})")
                else:
                    parts.append("-")
            f.write(f"| {name} | {' | '.join(parts)} |\n")

        # ── Per-case ──
        for name in config_names:
            r = results[name]
            f.write(f"\n## {name} — Case Details\n\n")
            f.write(f"**EX**: {r['ex_rate']:.1%} ({r['passed']}/{r['total']})  |  ")
            f.write(f"**VES**: {r['avg_ves']:.4f}  |  ")
            f.write(f"**Avg Time**: {r['avg_time_s']}s  |  ")
            f.write(f"**Avg Tokens**: {r['avg_tokens']}/query\n\n")
            f.write("| # | ID | Diff | Type | EX | VES | Detail | Att | Rev | Time | Tokens |\n")
            f.write("|---|-----|------|------|----|-----|--------|-----|-----|------|--------|\n")
            for i, c in enumerate(r["case_results"]):
                ex_icon = ":white_check_mark:" if c["ex"] else ":x:"
                f.write(f"| {i+1} | {c['id']} | {c['difficulty']} | {c['type']} | "
                        f"{ex_icon} | {c['ves']:.4f} | {c['detail']} | "
                        f"{c['exec_attempts']} | {c['review_rounds']} | "
                        f"{c['time_s']}s | {c['tokens']} |\n")

    return path
