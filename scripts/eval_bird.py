"""BIRD Mini-Dev evaluation — EX/VES/token tracking with ablation matrix.

All configs use the Full Graph pipeline. R0 = baseline, R1-R5 cumulatively add modules.

Usage:
  # Small validation test
  python scripts/eval_bird.py --test --samples 10

  # Full ablation (R0-R5)
  python scripts/eval_bird.py --exp ablation
"""
import argparse
import json
import math
import os
import sys
import time

# Force UTF-8 stdout to avoid UnicodeEncodeError (e.g., ⚠) on GBK terminals
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
# Monkey-patch Reviver default (allowed_objects=None → "core") so langgraph's
# module-level LC_REVIVER = Reviver() doesn't trigger LangChainPendingDeprecationWarning.
# Must run BEFORE any import that touches langgraph (langchain_core.load.load.Reviver).
import warnings as _warnings
try:
    from langchain_core.load.load import Reviver as _Reviver
    _defaults = _Reviver.__init__.__defaults__
    if _defaults and _defaults[0] is None:
        _Reviver.__init__.__defaults__ = ("core",) + _defaults[1:]
except Exception:
    _warnings.filterwarnings("ignore", message=".*allowed_objects.*")
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.eval.bird_loader import load_bird_dev, get_database_url, get_stats
from src.eval.metrics import exec_match, ves_score
from src.agent.graphs.full_graph import create_full_graph

# ── DeepSeek v4-pro pricing (per 1M tokens) ──────────────────────────────────
PRICE_INPUT = 0.28   # USD
PRICE_OUTPUT = 1.10  # USD


# ── Checkpoint helpers ────────────────────────────────────────────────────────

def _load_checkpoint(path: str) -> dict | None:
    """Load existing checkpoint if valid, else None."""
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and "results" in data:
            return data
    except Exception:
        pass
    return None


def _save_checkpoint(path: str, results: dict, knowledge_source: str):
    """Write checkpoint with all completed configs so far."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    checkpoint = {
        "knowledge_source": knowledge_source,
        "results": {},
    }
    for name, r in results.items():
        checkpoint["results"][name] = {
            "ex_rate": r["ex_rate"],
            "avg_ves": r["avg_ves"],
            "passed": r["passed"],
            "total": r["total"],
            "crashed": r.get("crashed", 0),
            "avg_tokens": r["avg_tokens"],
            "avg_time_s": r["avg_time_s"],
            "tokens_per_s": r["tokens_per_s"],
            "total_tokens": r["total_tokens"],
            "diff_summary": r.get("diff_summary", {}),
            "case_results": r.get("case_results", []),
            "router_traces": r.get("router_traces", []),
        }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, ensure_ascii=False, indent=2)


def _write_slow_log(case_results: list[dict], cfg_name: str, knowledge_source: str):
    """Write top-10 slowest samples to a timestamped log file for post-mortem."""
    if not case_results:
        return
    sorted_results = sorted(case_results, key=lambda r: r.get("elapsed_s", 0), reverse=True)
    slow = sorted_results[:10]
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports")
    os.makedirs(log_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(log_dir, f"slow_samples_{ts}_{cfg_name}_{knowledge_source}.json")
    entries = []
    for r in slow:
        entries.append({
            "question_id": r["question_id"],
            "db_id": r["db_id"],
            "difficulty": r["difficulty"],
            "elapsed_s": r["elapsed_s"],
            "ex": r["ex"],
            "ves": r["ves"],
            "detail": r.get("detail", "")[:200],
            "gen_sql": r.get("gen_sql", "")[:200],
            "gold_sql": r.get("gold_sql", "")[:200],
        })
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)
    avg_slow = sum(e["elapsed_s"] for e in entries) / len(entries)
    print(f"  Slowest 10 avg: {avg_slow:.1f}s → {log_path}")


def _write_timeout_log(timeout_log: list[dict], cfg_name: str, knowledge_source: str):
    """Write timeout events to a timestamped log file."""
    if not timeout_log:
        return
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports")
    os.makedirs(log_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(log_dir, f"timeout_events_{ts}_{cfg_name}_{knowledge_source}.json")
    # Group by db_id for pattern detection
    by_db: dict = defaultdict(lambda: {"count": 0, "samples": []})
    for e in timeout_log:
        by_db[e["db_id"]]["count"] += 1
        by_db[e["db_id"]]["samples"].append(e)
    summary = {
        "total_timeout_events": len(timeout_log),
        "by_db": {db: {"count": v["count"], "samples": v["samples"]} for db, v in sorted(by_db.items(), key=lambda x: -x[1]["count"])},
    }
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    crash_count = sum(1 for e in timeout_log if e["action"] == "crash")
    retry_count = sum(1 for e in timeout_log if e["action"] == "retry")
    print(f"  Timeout events: {retry_count} retry + {crash_count} crash across {len(by_db)} DBs → {log_path}")


def _write_trace_log(case_results: list[dict], cfg_name: str, knowledge_source: str):
    """Export per-sample trace events to a timestamped JSON file."""
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports")
    os.makedirs(log_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(log_dir, f"trace_{ts}_{cfg_name}_{knowledge_source}.json")
    export = {
        "cfg_name": cfg_name,
        "knowledge_source": knowledge_source,
        "timestamp": ts,
        "total_samples": len(case_results),
        "traces": [
            {
                "question_id": r["question_id"],
                "db_id": r["db_id"],
                "difficulty": r["difficulty"],
                "ex": r["ex"],
                "trace_events": r.get("trace_events", []),
            }
            for r in case_results
        ],
    }
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(export, f, ensure_ascii=False, default=str, indent=2)
    total_events = sum(len(t.get("trace_events", [])) for t in export["traces"])
    print(f"  Trace events: {total_events} total across {len(case_results)} samples → {log_path}")


def _load_previous_timeout_ids(output_dir: str) -> set[str]:
    """Scan report directory for previous timeout event logs, return affected question IDs."""
    timeout_ids: set[str] = set()
    if not os.path.isdir(output_dir):
        return timeout_ids
    for fname in os.listdir(output_dir):
        if not fname.startswith("timeout_events_") or not fname.endswith(".json"):
            continue
        fpath = os.path.join(output_dir, fname)
        try:
            with open(fpath, encoding="utf-8") as f:
                data = json.load(f)
            for db_info in data.get("by_db", {}).values():
                for evt in db_info.get("samples", []):
                    qid = evt.get("question_id")
                    if qid:
                        timeout_ids.add(str(qid))
        except Exception:
            pass
    return timeout_ids


# ── Default configs ──────────────────────────────────────────────────────────

def _default_configs() -> list[dict]:
    """All configs use Full Graph. Cumulative ablation: each step adds one module.

    R0: Baseline (no RAG, no MC, no fewshot, no decomposer)
    R1: +Decomposer (complex questions get decomposed)
    R2: +RAG (Schema + Domain merged)
    R3: +Multi-Candidate (3 temperatures + dedup)
    R4: +Prune & Few-shot (column prune + fewshot merged)
    R5:  Evidence (same as R4 but with BIRD gold evidence → theoretical ceiling)
    """
    return [
        {"name": "R0_Baseline",
         "rag_schema": False, "rag_domain": False, "sample_rows": False,
         "multi_candidate": False, "fewshot_enabled": False,
         "decomposer_enabled": False,
         "use_full_graph": True},
        {"name": "R1_Decomposer",
         "rag_schema": False, "rag_domain": False, "sample_rows": False,
         "multi_candidate": False, "fewshot_enabled": False,
         "decomposer_enabled": True,
         "use_full_graph": True},
        {"name": "R2_RAG",
         "rag_schema": True, "rag_domain": True, "sample_rows": True,
         "multi_candidate": False, "fewshot_enabled": False,
         "decomposer_enabled": True,
         "use_full_graph": True},
        {"name": "R3_MultiCandidate",
         "rag_schema": True, "rag_domain": True, "sample_rows": True,
         "multi_candidate": True, "fewshot_enabled": False,
         "decomposer_enabled": True,
         "use_full_graph": True},
        {"name": "R4_PruneFewshot",
         "rag_schema": True, "rag_domain": True, "sample_rows": True,
         "multi_candidate": True, "rag_column_prune": True, "fewshot_enabled": True,
         "decomposer_enabled": True,
         "use_full_graph": True},
        {"name": "R5_Evidence",
         "rag_schema": True, "rag_domain": True, "sample_rows": True,
         "multi_candidate": True, "rag_column_prune": True, "fewshot_enabled": True,
         "decomposer_enabled": True,
         "use_full_graph": True,
         "knowledge_source": "evidence"},
    ]


def _get_domain_override(sample, knowledge_source: str) -> str:
    """Return domain notes override for knowledge ablation experiment 2.

    - "evidence": BIRD human-written hints (gold standard)
    - "none" / "rag": no override — schema_retriever handles via db_id
    """
    if knowledge_source == "evidence":
        return sample.evidence or ""
    return ""


# ── Single sample evaluation ────────────────────────────────────────────────

def evaluate_bird_sample(sample, config: dict, knowledge_source: str, data_dir: str,
                         log_dir: str | None = None,
                         gold_cache_map: dict[str, dict] | None = None) -> dict:
    """Run one Full Graph pipeline config against one BIRD sample.

    gold_cache_map: {question_id: {norm, gold_time_ms, gold_rows}}
        Precomputed gold results, avoids re-executing gold SQL."""
    from src.obs.logger import TraceLogger

    database_url = get_database_url(sample)

    # Knowledge source overrides config flags
    rag_schema = config.get("rag_schema", False)
    rag_domain = config.get("rag_domain", False)
    if knowledge_source == "none":
        rag_schema = False
        rag_domain = False

    domain_override = _get_domain_override(sample, knowledge_source)

    # Create per-sample trace logger — streams events to disk immediately
    tlog = TraceLogger(log_dir=log_dir)

    t0 = time.time()
    graph_error = ""
    node_timings = {}
    trace_events = []
    last_graph_node = "?"
    try:
        graph = create_full_graph()
        state = graph.invoke({
            "question": sample.question,
            "db_id": sample.db_id,
            "rag_schema": rag_schema,
            "rag_domain": rag_domain,
            "skip_schema": config.get("skip_schema", False),
            "sample_rows": config.get("sample_rows", True),
            "multi_candidate": config.get("multi_candidate", False),
            "rag_k": config.get("k", 8),
            "rag_column_prune": config.get("rag_column_prune", False),
            "fewshot_enabled": config.get("fewshot_enabled", False),
            "decomposer_enabled": config.get("decomposer_enabled", False),
            "database_url": database_url,
            "_domain_notes_override": domain_override,
            "tlog": tlog,
        })
        gen_sql = state.get("sql", "")
        tu = state.get("token_usage", {})
        node_timings = tlog.get_node_timings()
        trace_events = list(tlog.events)
        # Last graph node that exited (or entered if no exit)
        for ev in reversed(trace_events):
            if ev["event"] in ("node_exit", "node_enter"):
                last_graph_node = ev.get("node", "?")
                break
    except Exception as e:
        gen_sql = ""
        tu = {}
        state = {}
        graph_error = f"{type(e).__name__}: {str(e)[:180]}"
        for ev in reversed(tlog.events):
            if ev["event"] == "node_enter":
                last_graph_node = ev.get("node", "?")
                break
        tlog.node_error(last_graph_node, type(e).__name__, str(e)[:300])
    t1 = time.time()

    if gen_sql:
        gold_entry = gold_cache_map.get(sample.question_id) if gold_cache_map else None
        ex_info = exec_match(sample.gold_sql, gen_sql, database_url=database_url,
                             gold_cache=gold_entry)
    else:
        err_detail = graph_error or "SQL generation failed"
        ex_info = {"ex": False, "detail": err_detail,
                   "gold_time_ms": 0, "gen_time_ms": 0, "gold_rows": 0, "gen_rows": 0}
    t2 = time.time()

    elapsed_graph_s = round(t1 - t0, 3)
    elapsed_execmatch_s = round(t2 - t1, 4)
    elapsed_total = round(t2 - t0, 2)

    ex = ex_info["ex"]
    v = ves_score(ex, ex_info["gold_time_ms"], ex_info["gen_time_ms"])

    # ── Pipeline module stats (harvested from state after graph invoke) ──
    guard_pass = state.get("guard_pass")
    guard_issues = state.get("guard_issues", [])
    semantic_pass = state.get("semantic_pass")
    decomposer_used = bool(
        state.get("complexity") == "complex" and state.get("decomposer_enabled")
    )

    # ── RAG recall ──
    from src.eval.metrics import compute_rag_recall
    rag_chunks = state.get("rag_chunks", [])
    rag_recall_info = compute_rag_recall(rag_chunks, sample.gold_sql)

    return {
        "question_id": sample.question_id,
        "db_id": sample.db_id,
        "difficulty": sample.difficulty,
        "knowledge_source": knowledge_source,
        "ex": ex,
        "ves": round(v, 4),
        "detail": ex_info["detail"],
        "gen_sql": gen_sql[:300] if gen_sql else "",
        "gold_sql": sample.gold_sql[:300],
        "token_usage": tu,
        "elapsed_s": elapsed_total,
        "elapsed_graph_s": elapsed_graph_s,
        "elapsed_execmatch_s": elapsed_execmatch_s,
        "last_graph_node": last_graph_node,
        "node_timings": node_timings,
        "trace_events": trace_events,
        "router_score": state.get("router_score"),
        "router_method": state.get("router_method"),
        "router_complexity": state.get("complexity"),
        "trace_id": tlog.trace_id,
        # ── Pipeline module tracking ──
        "guard_pass": guard_pass,
        "guard_issue_count": len(guard_issues),
        "ast_pass": state.get("ast_pass"),
        "semantic_pass": semantic_pass,
        "semantic_feedback": state.get("semantic_feedback", "")[:200],
        "sem_reject_count": state.get("_sem_reject_count", 0),
        "retry_count": state.get("retry_count", 0),
        "candidate_count": len(state.get("candidate_sqls", [])),
        "decomposer_used": decomposer_used,
        "sub_question_count": len(state.get("sub_questions", [])),
        # ── RAG recall ──
        "rag_recall": rag_recall_info["recall"] if rag_recall_info else None,
        "rag_recall_detail": rag_recall_info,
    }


# ── Pipeline stats aggregation ─────────────────────────────────────────────

def _candidate_dist(case_results: list[dict]) -> dict[str, int]:
    """Count samples with 1, 2, or 3 unique candidates."""
    dist = {"1": 0, "2": 0, "3": 0}
    for r in case_results:
        n = r.get("candidate_count", 0)
        if n >= 1:
            dist[str(min(n, 3))] += 1
    return dist


def _compute_pipeline_stats(case_results: list[dict]) -> dict:
    """Aggregate pipeline module statistics from per-sample results."""
    n = len(case_results)
    if n == 0:
        return {}

    # ── Guard ──
    guard_checked = [r for r in case_results if r.get("guard_pass") is not None]
    guard_total = len(guard_checked)
    guard_passed = sum(1 for r in guard_checked if r["guard_pass"])
    guard_rejected = guard_total - guard_passed
    guard_fn = sum(1 for r in guard_checked
                   if r["guard_pass"] and not r["ex"] and r.get("gen_sql"))

    # ── Sem Check ──
    sem_checked = [r for r in case_results if r.get("semantic_pass") is not None]
    sem_total = len(sem_checked)
    sem_passed = sum(1 for r in sem_checked if r["semantic_pass"])
    sem_rejected = sem_total - sem_passed
    sem_fn = sum(1 for r in sem_checked
                 if r["semantic_pass"] and not r["ex"] and r.get("gen_sql"))
    sem_fp = sum(1 for r in sem_checked
                 if r["semantic_pass"] is False and r["ex"])
    sem_escape = sum(1 for r in sem_checked
                     if r.get("sem_reject_count", 0) >= 2)

    # ── Self-Correction ──
    retried = sum(1 for r in case_results if r.get("retry_count", 0) > 0)
    retry_fixed = sum(1 for r in case_results
                      if r.get("retry_count", 0) > 0 and r["ex"])

    # ── Voter ──
    multi_samples = [r for r in case_results if r.get("candidate_count", 0) > 0]
    avg_candidates = sum(r.get("candidate_count", 0) for r in case_results) / n if n else 0

    # ── Decomposer ──
    dec_used = sum(1 for r in case_results if r.get("decomposer_used"))
    dec_ex = sum(1 for r in case_results if r.get("decomposer_used") and r["ex"])

    # ── Node timing distribution (avg per node across all samples) ──
    node_times: dict[str, list[float]] = {}
    for r in case_results:
        for node, dur in r.get("node_timings", {}).items():
            node_times.setdefault(node, []).append(dur)
    node_avg = {k: round(sum(v) / len(v), 2) for k, v in sorted(node_times.items())}

    return {
        "guard": {
            "total_checked": guard_total,
            "passed": guard_passed, "rejected": guard_rejected,
            "reject_rate": round(guard_rejected / guard_total, 3) if guard_total else 0,
            "false_negatives": guard_fn,
            "false_neg_rate": round(guard_fn / guard_total, 3) if guard_total else 0,
        },
        "sem_check": {
            "total_checked": sem_total,
            "passed": sem_passed, "rejected": sem_rejected,
            "reject_rate": round(sem_rejected / sem_total, 3) if sem_total else 0,
            "false_negatives": sem_fn,
            "false_neg_rate": round(sem_fn / sem_total, 3) if sem_total else 0,
            "false_positives": sem_fp,
            "escape_hatch_triggers": sem_escape,
        },
        "self_correction": {
            "retried": retried,
            "retry_fixed": retry_fixed,
            "fix_rate": round(retry_fixed / retried, 3) if retried else 0,
            "retry_pct": round(retried / n, 3) if n else 0,
        },
        "voter": {
            "multi_enabled_samples": len(multi_samples),
            "avg_candidates": round(avg_candidates, 2),
            "candidate_distribution": _candidate_dist(case_results),
        },
        "decomposer": {
            "used": dec_used,
            "ex": dec_ex,
            "ex_rate": round(dec_ex / dec_used, 3) if dec_used else 0,
            "avg_sub_questions": round(
                sum(r.get("sub_question_count", 0) for r in case_results) / n, 1
            ) if n else 0,
        },
        "node_timing_avg": node_avg,
    }


# ── Batch evaluation ────────────────────────────────────────────────────────

def run_bird_eval(
    samples: list,
    configs: list[dict],
    knowledge_source: str = "rag",
    data_dir: str | None = None,
    max_workers: int = 8,
    progress_interval: int = 60,
    checkpoint_path: str | None = None,
) -> dict:
    """Run evaluation over samples for a single config set + knowledge source.

    Returns per-config results with full token tracking.
    Saves checkpoint after each config if checkpoint_path is provided.
    """
    default_data = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "bird", "mini_dev_data", "minidev", "MINIDEV",
    )
    _data_dir = data_dir or default_data

    # Per-run trace log directory
    _trace_root = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "logs", "traces",
    )
    _log_dir = os.path.join(_trace_root, datetime.now().strftime("%Y%m%d_%H%M%S"))
    os.makedirs(_log_dir, exist_ok=True)
    # Auto-cleanup: keep only last 5 trace directories
    from src.obs.logger import cleanup_trace_dirs
    _cleaned = cleanup_trace_dirs(_trace_root, keep=5)
    if _cleaned:
        print(f"Cleaned {_cleaned} old trace dir(s)")
    print(f"Trace logs: {_log_dir}")

    all_results = {}

    # ── Load gold result cache (precomputed by scripts/_precompute_gold.py) ──
    gold_cache_map: dict[str, dict] = {}
    _gold_cache_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "reports", ".gold_cache",
    )
    if os.path.isdir(_gold_cache_dir):
        for fname in os.listdir(_gold_cache_dir):
            if fname.endswith(".json"):
                with open(os.path.join(_gold_cache_dir, fname), "r", encoding="utf-8") as f:
                    gold_cache_map.update(json.load(f))
        if gold_cache_map:
            n_ok = sum(1 for v in gold_cache_map.values() if v.get("norm") is not None)
            n_fail = len(gold_cache_map) - n_ok
            print(f"Gold cache loaded: {len(gold_cache_map)} entries ({n_ok} ok, {n_fail} unavailable)")

    for cfg_idx, cfg in enumerate(configs):
        cfg_name = cfg["name"]
        n = len(samples)
        print(f"\n{'='*60}")
        cfg_knowledge = cfg.get("knowledge_source", knowledge_source)
        print(f"Config {cfg_idx+1}/{len(configs)}: {cfg_name}  |  source={cfg_knowledge}  |  {n} samples")
        print(f"{'='*60}")

        cfg_start = time.time()
        case_results: list[dict] = []
        passed = 0
        crashed = 0
        completed = 0
        total_time = 0.0
        total_ves = 0.0
        total_tokens = {"prompt": 0, "completion": 0, "total": 0}
        diff_stats: dict = defaultdict(lambda: {"passed": 0, "total": 0, "ves_sum": 0.0,
                                                "tokens": 0, "time": 0.0})
        # Router tracking (Full Graph only)
        router_traces: list[dict] = []
        # RAG recall aggregation (only for configs with RAG enabled)
        rag_recall_sum = 0.0
        rag_recall_count = 0
        timeout_log: list[dict] = []  # per-timeout events for post-mortem
        MAX_SAMPLE_RETRIES = 2
        # Progressive hard timeout per sample attempt: tight → loose → loosest
        TIMEOUTS = [120, 300, 480]  # seconds

        pool = ThreadPoolExecutor(max_workers=max_workers)
        # task_queue: samples waiting to start (not submitted yet)
        # Each entry is (sample, retries)
        from collections import deque
        task_queue = deque([(s, 0) for s in samples])

        def _submit_next():
            """Submit one task from queue if slots available."""
            if task_queue and len(pending) < max_workers:
                s, retries = task_queue.popleft()
                fut = pool.submit(evaluate_bird_sample, s, cfg, cfg_knowledge, _data_dir, _log_dir, gold_cache_map)
                pending[fut] = (s, retries, time.time())

        _interrupted = [False]  # mutable so except/finally share it
        try:
            # pending: future → (sample, retries, submit_time) — only running ones
            pending: dict = {}

            # Fill initial slots
            while len(pending) < max_workers and task_queue:
                _submit_next()

            last_print = time.time()
            while pending:
                # Wait with 15s heartbeat — wakes even when all workers are stuck
                done, _ = wait(
                    pending, timeout=15, return_when=FIRST_COMPLETED
                )
                now = time.time()

                # Check for futures that exceeded hard timeout (stale/runaway)
                expired = []
                for fut, (sample, retries, t_submit) in list(pending.items()):
                    hard_timeout = TIMEOUTS[min(retries, len(TIMEOUTS) - 1)]
                    if now - t_submit > hard_timeout:
                        expired.append((fut, sample, retries))

                # Process normally completed futures
                for future in done:
                    if future not in pending:
                        continue
                    sample, retries, _ = pending.pop(future)
                    try:
                        r = future.result(timeout=0)
                    except Exception as exc:
                        if retries < MAX_SAMPLE_RETRIES:
                            print(f"  [RETRY] #{sample.question_id} db={sample.db_id} attempt={retries+1}: {type(exc).__name__}: {str(exc)[:120]}")
                            task_queue.appendleft((sample, retries + 1))
                            future.cancel()
                            continue
                        else:
                            crashed += 1
                            r = {
                                "question_id": sample.question_id,
                                "db_id": sample.db_id,
                                "difficulty": sample.difficulty,
                                "knowledge_source": cfg_knowledge,
                                "ex": False, "ves": 0.0,
                                "detail": f"Failed after {MAX_SAMPLE_RETRIES + 1} attempts: {exc}"[:200],
                                "gen_sql": "", "gold_sql": sample.gold_sql[:300],
                                "token_usage": {}, "elapsed_s": 0,
                                "elapsed_graph_s": 0, "elapsed_execmatch_s": 0,
                                "last_graph_node": "?", "node_timings": {}, "trace_events": [],
                                "router_score": None, "router_method": None, "router_complexity": None,
                                "trace_id": "",
                                "guard_pass": None, "guard_issue_count": 0, "ast_pass": None,
                                "semantic_pass": None, "semantic_feedback": "", "sem_reject_count": 0,
                                "retry_count": 0, "candidate_count": 0,
                                "decomposer_used": False, "sub_question_count": 0,
                                "rag_recall": None, "rag_recall_detail": None,
                            }
                    completed += 1
                    elapsed = r["elapsed_s"]
                    total_time += elapsed
                    tu = r.get("token_usage", {})
                    if tu:
                        for k in total_tokens:
                            total_tokens[k] += tu.get(k, 0)
                    ex = r["ex"]
                    v = r["ves"]
                    total_ves += v
                    d = r.get("difficulty", "?")
                    if ex:
                        passed += 1
                        diff_stats[d]["passed"] += 1
                        diff_stats[d]["ves_sum"] += v
                    diff_stats[d]["total"] += 1
                    diff_stats[d]["tokens"] += tu.get("total", 0)
                    diff_stats[d]["time"] += elapsed
                    case_results.append(r)
                    if r.get("rag_recall") is not None:
                        rag_recall_sum += r["rag_recall"]
                        rag_recall_count += 1
                    if r.get("router_score") is not None:
                        router_traces.append({
                            "question_id": r["question_id"],
                            "difficulty": r["difficulty"],
                            "router_score": r["router_score"],
                            "router_method": r["router_method"],
                            "router_complexity": r["router_complexity"],
                            "ex_pass": ex,
                        })

                # Process expired (hard-timeout) futures
                for fut, sample, retries in expired:
                    if fut not in pending:
                        continue
                    del pending[fut]
                    fut.cancel()
                    elapsed_t = round(now - t_submit, 1)
                    hard = TIMEOUTS[min(retries, len(TIMEOUTS) - 1)]
                    if retries < MAX_SAMPLE_RETRIES:
                        t_evt = {
                            "question_id": sample.question_id,
                            "db_id": sample.db_id,
                            "difficulty": sample.difficulty,
                            "retry": retries + 1,
                            "timeout_s": hard,
                            "action": "retry",
                        }
                        timeout_log.append(t_evt)
                        print(f"  [TIMEOUT] #{sample.question_id} db={sample.db_id} attempt={retries+1}/{MAX_SAMPLE_RETRIES+1} ({hard}s) → retry")
                        task_queue.appendleft((sample, retries + 1))
                    else:
                        t_evt = {
                            "question_id": sample.question_id,
                            "db_id": sample.db_id,
                            "difficulty": sample.difficulty,
                            "retry": retries + 1,
                            "timeout_s": hard,
                            "action": "crash",
                        }
                        timeout_log.append(t_evt)
                        print(f"  [TIMEOUT] #{sample.question_id} db={sample.db_id} attempt={retries+1}/{MAX_SAMPLE_RETRIES+1} ({hard}s) → CRASH")
                        crashed += 1
                        r = {
                            "question_id": sample.question_id,
                            "db_id": sample.db_id,
                            "difficulty": sample.difficulty,
                            "knowledge_source": cfg_knowledge,
                            "ex": False, "ves": 0.0,
                            "detail": f"Hard timeout after {MAX_SAMPLE_RETRIES + 1} attempts"[:200],
                            "gen_sql": "", "gold_sql": sample.gold_sql[:300],
                            "token_usage": {}, "elapsed_s": 0,
                            "elapsed_graph_s": 0, "elapsed_execmatch_s": 0,
                            "last_graph_node": "?", "node_timings": {}, "trace_events": [],
                            "router_score": None, "router_method": None, "router_complexity": None,
                            "trace_id": "",
                            "guard_pass": None, "guard_issue_count": 0, "ast_pass": None,
                            "semantic_pass": None, "semantic_feedback": "", "sem_reject_count": 0,
                            "retry_count": 0, "candidate_count": 0,
                            "decomposer_used": False, "sub_question_count": 0,
                            "rag_recall": None, "rag_recall_detail": None,
                        }
                        completed += 1
                        d = r.get("difficulty", "?")
                        diff_stats[d]["total"] += 1
                        case_results.append(r)

                # Refill slots from queue
                while len(pending) < max_workers and task_queue:
                    _submit_next()

                # Heartbeat: always prints, even when no futures completed
                if now - last_print >= progress_interval or completed == n:
                    last_print = now
                    pct = completed / n * 100
                    cur_ex = passed / completed if completed else 0
                    cur_tok = total_tokens["total"] / completed if completed else 0
                    crash_str = f" | ⚠{crashed} crash" if crashed else ""
                    queue_str = f" +{len(task_queue)} queued" if task_queue else ""
                    active_str = f" | {len(pending)} running{queue_str}" if completed < n else ""
                    print(f"  [{completed}/{n} {pct:.0f}%] EX:{passed}/{completed}={cur_ex:.1%} | {cur_tok:.0f} tok/q | elapsed {now - cfg_start:.0f}s{crash_str}{active_str}")

        except KeyboardInterrupt:
            print(f"\n  Interrupted at {completed}/{n}, {crashed} crashed")
            if pending:
                print(f"  {len(pending)} running worker(s)")
                now = time.time()
                for fut, (sample, retries, t_submit) in list(pending.items()):
                    elapsed = now - t_submit
                    hard = TIMEOUTS[min(retries, len(TIMEOUTS) - 1)]
                    print(f"    #{sample.question_id}  db={sample.db_id}  attempt={retries+1}  elapsed={elapsed:.0f}s  timeout={hard}s  diff={sample.difficulty}")
            print(f"  {len(task_queue)} queued (not started)")
            print("  Saving partial results...")
            _interrupted[0] = True
            raise  # re-raise to stop outer loop
        finally:
            # Timeout-guarded shutdown. When interrupted, skip the wait —
            # daemon threads will be killed on process exit.
            import threading
            if _interrupted[0]:
                pool.shutdown(wait=False, cancel_futures=True)
                print("  ⚡ Skipped pool shutdown wait (interrupted)")
            else:
                shutdown_ok = threading.Event()
                def _shutdown():
                    pool.shutdown(wait=True, cancel_futures=True)
                    shutdown_ok.set()
                t = threading.Thread(target=_shutdown, daemon=True)
                t.start()
                if not shutdown_ok.wait(timeout=10):
                    print("  Pool shutdown >10s (harmless: threads will exit on process termination)")

        # Sort by question_id for report consistency
        case_results.sort(key=lambda c: c["question_id"])

        diff_summary = {}
        for d in ["simple", "moderate", "challenging"]:
            ds = diff_stats[d]
            if ds["total"] > 0:
                diff_summary[d] = {
                    "total": ds["total"],
                    "passed": ds["passed"],
                    "ex": round(ds["passed"] / ds["total"], 4),
                    "avg_ves": round(ds["ves_sum"] / ds["total"], 4),
                    "avg_tokens": round(ds["tokens"] / ds["total"]),
                    "avg_time": round(ds["time"] / ds["total"], 2),
                }

        avg_tok = round(total_tokens["total"] / n) if n else 0

        # ── Pipeline module statistics ──
        pipeline_stats = _compute_pipeline_stats(case_results)

        # Write timeout log to file
        if timeout_log:
            _write_timeout_log(timeout_log, cfg_name, cfg_knowledge)

        avg_rag_recall = round(rag_recall_sum / rag_recall_count, 4) if rag_recall_count else None
        all_results[cfg_name] = {
            "config": cfg,
            "knowledge_source": cfg_knowledge,
            "case_results": case_results,
            "timeout_log": timeout_log,
            "passed": passed,
            "crashed": crashed,
            "total": n,
            "ex_rate": round(passed / n, 4) if n else 0,
            "avg_ves": round(total_ves / n, 4) if n else 0,
            "avg_time_s": round(total_time / n, 2) if n else 0,
            "total_tokens": total_tokens,
            "avg_tokens": avg_tok,
            "tokens_per_s": round(avg_tok / (total_time / n), 1) if (n and total_time > 0) else 0,
            "diff_summary": diff_summary,
            "router_traces": router_traces,
            "pipeline_stats": pipeline_stats,
            "avg_rag_recall": avg_rag_recall,
            "rag_recall_samples": rag_recall_count,
        }

        ex_pct = all_results[cfg_name]["ex_rate"]
        tps = all_results[cfg_name]["tokens_per_s"]
        crash_note = f"  |  Crashed: {crashed}" if crashed else ""
        print(f"\n  EX: {passed}/{n} = {ex_pct:.1%}  |  VES: {all_results[cfg_name]['avg_ves']:.4f}  |  avg {total_time/n:.1f}s/q  |  avg {avg_tok} tok/q  |  {tps:.0f} tok/s{crash_note}")
        if avg_rag_recall is not None:
            print(f"  RAG Table Recall: {avg_rag_recall:.1%} ({rag_recall_count}/{n} samples)")

        # ── Print pipeline summary (compact) ──
        g = pipeline_stats.get("guard", {})
        s = pipeline_stats.get("sem_check", {})
        c = pipeline_stats.get("self_correction", {})
        if g:
            print(f"  Guard: {g['rejected']}/{g['total_checked']} rejected ({g['reject_rate']:.1%})"
                  f"  |  FN: {g['false_negatives']} ({g['false_neg_rate']:.1%})")
        if s:
            print(f"  SemCheck: {s['rejected']}/{s['total_checked']} rejected ({s['reject_rate']:.1%})"
                  f"  |  FN: {s['false_negatives']} ({s['false_neg_rate']:.1%})"
                  f"  |  Escape: {s['escape_hatch_triggers']}")
        if c["retried"] > 0:
            print(f"  Self-Correction: {c['retried']} retried ({c['retry_pct']:.1%}),"
                  f"  {c['retry_fixed']} fixed ({c['fix_rate']:.1%})")
        vt = pipeline_stats.get("voter", {})
        if vt and vt.get("multi_enabled_samples", 0) > 0:
            cd = vt.get("candidate_distribution", {})
            print(f"  Voter: avg {vt['avg_candidates']} candidates"
                  f"  |  dist 1/2/3: {cd.get('1',0)}/{cd.get('2',0)}/{cd.get('3',0)}")
        dc = pipeline_stats.get("decomposer", {})
        if dc.get("used", 0) > 0:
            print(f"  Decomposer: {dc['used']} samples, EX={dc['ex_rate']:.1%}"
                  f"  |  avg {dc['avg_sub_questions']} sub-questions")
        # ── Print timing split (graph vs exec_match) ──
        timings = [(r.get("elapsed_graph_s", 0), r.get("elapsed_execmatch_s", 0),
                     r.get("last_graph_node", "?"))
                   for r in case_results if r.get("elapsed_graph_s", 0) > 0]
        if timings:
            avg_graph = round(sum(t[0] for t in timings) / len(timings), 2)
            avg_exec = round(sum(t[1] for t in timings) / len(timings), 4)
            print(f"  Timing: graph={avg_graph:.1f}s + exec_match={avg_exec:.3f}s avg"
                  f"  |  graph_pct={avg_graph/(avg_graph+avg_exec)*100:.0f}%")
            # Slowest exec_match
            slowest = sorted(timings, key=lambda t: -t[1])[:3]
            for gt, em, node in slowest:
                if em > 0.5:
                    print(f"    slow exec_match: {em:.2f}s (graph={gt:.1f}s, last_node={node})")

        nt = pipeline_stats.get("node_timing_avg", {})
        if nt:
            top_nodes = sorted(nt.items(), key=lambda x: x[1], reverse=True)[:4]
            print(f"  Node time: {' | '.join(f'{k}={v:.1f}s' for k, v in top_nodes)}")

        # Save slowest samples to log for post-mortem
        _write_slow_log(case_results, cfg_name, cfg_knowledge)

        # Save per-sample trace events for offline analysis
        _write_trace_log(case_results, cfg_name, cfg_knowledge)

        # Save checkpoint after each config
        if checkpoint_path:
            _save_checkpoint(checkpoint_path, all_results, cfg_knowledge)

    # Cleanup: dispose all cached SQLAlchemy engines so connection pool threads
    # don't block the process from exiting.
    from nl2sql.schema import get_engine
    try:
        # lru_cache doesn't expose values, so we warm + dispose the known BIRD DB urls
        from src.eval.bird_loader import get_database_url
        seen = set()
        for s in samples:
            url = get_database_url(s)
            if url not in seen:
                seen.add(url)
                try:
                    eng = get_engine(url)
                    eng.dispose()
                except Exception:
                    pass
    except Exception:
        pass
    return all_results


# ── Cost estimation ─────────────────────────────────────────────────────────

def estimate_cost(results: dict, total_samples: int) -> dict:
    """Estimate token cost and predict full-run cost."""
    total_prompt = sum(r["total_tokens"]["prompt"] for r in results.values())
    total_completion = sum(r["total_tokens"]["completion"] for r in results.values())
    total_all = sum(r["total_tokens"]["total"] for r in results.values())

    input_cost = total_prompt / 1_000_000 * PRICE_INPUT
    output_cost = total_completion / 1_000_000 * PRICE_OUTPUT
    total_cost = input_cost + output_cost

    samples_run = next(iter(results.values()))["total"]
    configs_run = len(results)
    calls_run = samples_run * configs_run

    avg_tokens_per_sample = total_all / calls_run if calls_run else 0

    # Predict for full 500-sample run with the same configs
    full_calls = total_samples * configs_run
    full_tokens_est = avg_tokens_per_sample * full_calls
    full_cost_est = (full_tokens_est / total_all * total_cost) if total_all else 0

    return {
        "test_run": {
            "samples": samples_run,
            "configs": configs_run,
            "total_calls": calls_run,
            "total_tokens": total_all,
            "prompt_tokens": total_prompt,
            "completion_tokens": total_completion,
            "cost_input_usd": round(input_cost, 4),
            "cost_output_usd": round(output_cost, 4),
            "cost_total_usd": round(total_cost, 4),
        },
        "full_run_prediction": {
            "samples": total_samples,
            "configs": configs_run,
            "total_calls": full_calls,
            "estimated_tokens": round(full_tokens_est),
            "estimated_cost_usd_best": round(full_cost_est, 2),
            "estimated_cost_usd_with_retries": round(full_cost_est * 2.5, 2),
        },
        "avg_tokens_per_sample": round(avg_tokens_per_sample),
        "pricing": {"input_per_1M": PRICE_INPUT, "output_per_1M": PRICE_OUTPUT, "model": "deepseek-v4-pro"},
    }


# ── Report generation ───────────────────────────────────────────────────────

def _write_pipeline_md(f, results: dict, config_names: list[str]):
    """Write pipeline module performance sections to markdown report."""

    # ── Guard ──
    f.write("\n## Guard (Hard SQL Validation)\n\n")
    f.write("| Config | Checked | Passed | Rejected | Reject Rate | FN (Pass→EX=0) | FN Rate |\n")
    f.write("|--------|---------|--------|----------|-------------|-----------------|---------|\n")
    for name in config_names:
        g = results[name].get("pipeline_stats", {}).get("guard", {})
        if g:
            f.write(f"| {name} | {g['total_checked']} | {g['passed']} | {g['rejected']} | "
                    f"{g['reject_rate']:.1%} | {g['false_negatives']} | {g['false_neg_rate']:.1%} |\n")

    # ── Sem Check ──
    f.write("\n## Semantic Check (LLM Binary YES/NO)\n\n")
    f.write("| Config | Checked | Passed | Rejected | Reject Rate | FN (YES→EX=0) | FN Rate | "
            "FP (NO→EX=1) | Escape Hatch |\n")
    f.write("|--------|---------|--------|----------|-------------|----------------|---------|"
            "--------------|---------------|\n")
    for name in config_names:
        s = results[name].get("pipeline_stats", {}).get("sem_check", {})
        if s:
            f.write(f"| {name} | {s['total_checked']} | {s['passed']} | {s['rejected']} | "
                    f"{s['reject_rate']:.1%} | {s['false_negatives']} | {s['false_neg_rate']:.1%} | "
                    f"{s['false_positives']} | {s['escape_hatch_triggers']} |\n")

    # ── Self-Correction ──
    f.write("\n## Self-Correction (Refiner→Generator Loop)\n\n")
    f.write("| Config | Retried | Retry % | Fixed | Fix Rate |\n")
    f.write("|--------|---------|---------|-------|----------|\n")
    for name in config_names:
        c = results[name].get("pipeline_stats", {}).get("self_correction", {})
        if c:
            f.write(f"| {name} | {c['retried']} | {c['retry_pct']:.1%} | "
                    f"{c['retry_fixed']} | {c['fix_rate']:.1%} |\n")

    # ── Voter ──
    has_multi = any(
        results[n].get("pipeline_stats", {}).get("voter", {}).get("multi_enabled_samples", 0) > 0
        for n in config_names
    )
    if has_multi:
        f.write("\n## Voter (Multi-Candidate Execution Voting)\n\n")
        f.write("| Config | Multi Samples | Avg Candidates | Dist 1 | Dist 2 | Dist 3 |\n")
        f.write("|--------|---------------|----------------|--------|--------|--------|\n")
        for name in config_names:
            v = results[name].get("pipeline_stats", {}).get("voter", {})
            if v and v.get("multi_enabled_samples", 0) > 0:
                cd = v.get("candidate_distribution", {})
                f.write(f"| {name} | {v['multi_enabled_samples']} | {v['avg_candidates']} | "
                        f"{cd.get('1', 0)} | {cd.get('2', 0)} | {cd.get('3', 0)} |\n")

    # ── Decomposer ──
    has_dec = any(
        results[n].get("pipeline_stats", {}).get("decomposer", {}).get("used", 0) > 0
        for n in config_names
    )
    if has_dec:
        f.write("\n## Decomposer (Complex Question Decomposition)\n\n")
        f.write("| Config | Samples | EX | EX Rate | Avg Sub-Questions |\n")
        f.write("|--------|---------|-----|---------|-------------------|\n")
        for name in config_names:
            d = results[name].get("pipeline_stats", {}).get("decomposer", {})
            if d and d.get("used", 0) > 0:
                f.write(f"| {name} | {d['used']} | {d['ex']} | {d['ex_rate']:.1%} | "
                        f"{d['avg_sub_questions']} |\n")

    # ── Node Timing ──
    f.write("\n## Node Timing (Avg per Sample)\n\n")
    # Collect all node names across configs
    all_nodes: set[str] = set()
    for name in config_names:
        nt = results[name].get("pipeline_stats", {}).get("node_timing_avg", {})
        all_nodes.update(nt.keys())
    if all_nodes:
        sorted_nodes = sorted(all_nodes)
        f.write("| Config | " + " | ".join(sorted_nodes) + " |\n")
        f.write("|--------|" + "|".join(["--------"] * len(sorted_nodes)) + "|\n")
        for name in config_names:
            nt = results[name].get("pipeline_stats", {}).get("node_timing_avg", {})
            vals = [f"{nt.get(n, 0):.1f}s" for n in sorted_nodes]
            f.write(f"| {name} | " + " | ".join(vals) + " |\n")

    # ── RAG Table Recall ──
    has_rag_recall = any(results[n].get("avg_rag_recall") is not None for n in config_names)
    if has_rag_recall:
        f.write("\n## RAG Table Recall\n\n")
        f.write("| Config | Samples | Avg Recall |\n")
        f.write("|--------|---------|------------|\n")
        for name in config_names:
            r = results[name]
            rr = r.get("avg_rag_recall")
            if rr is not None:
                f.write(f"| {name} | {r.get('rag_recall_samples', 0)} | {rr:.1%} |\n")


def write_bird_report(results: dict, cost: dict, output_dir: str, experiment: str = "") -> str:
    """Write summary.json, summary.md, cost_report.json."""
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    config_names = list(results.keys())
    n_total = results[config_names[0]]["total"]

    # ── summary.json ──
    summary_json = {
        "experiment": experiment,
        "timestamp": ts,
        "total_samples": n_total,
        "configs": {}
    }
    for name in config_names:
        r = results[name]
        traces = r.get("router_traces", [])
        router_dist = {}
        if traces:
            s_count = sum(1 for t in traces if t.get("router_complexity") == "simple")
            c_count = sum(1 for t in traces if t.get("router_complexity") == "complex")
            router_dist = {"simple": s_count, "complex": c_count, "total": len(traces)}
            # EX breakdown by router classification
            s_pass = sum(1 for t in traces if t.get("router_complexity") == "simple" and t.get("ex_pass"))
            c_pass = sum(1 for t in traces if t.get("router_complexity") == "complex" and t.get("ex_pass"))
            router_dist["simple_ex"] = round(s_pass / s_count, 4) if s_count else 0
            router_dist["complex_ex"] = round(c_pass / c_count, 4) if c_count else 0
        summary_json["configs"][name] = {
            "ex_rate": r["ex_rate"],
            "avg_ves": r["avg_ves"],
            "passed": r["passed"],
            "total": r["total"],
            "crashed": r.get("crashed", 0),
            "avg_tokens": r["avg_tokens"],
            "avg_time_s": r["avg_time_s"],
            "tokens_per_s": r["tokens_per_s"],
            "total_tokens": r["total_tokens"],
            "diff_summary": r["diff_summary"],
            "router_distribution": router_dist,
            "pipeline_stats": r.get("pipeline_stats", {}),
            "avg_rag_recall": r.get("avg_rag_recall"),
            "rag_recall_samples": r.get("rag_recall_samples", 0),
        }

    with open(os.path.join(output_dir, f"bird_{experiment}_{ts}_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary_json, f, ensure_ascii=False, indent=2)

    # ── cost_report.json ──
    with open(os.path.join(output_dir, f"bird_{experiment}_{ts}_cost.json"), "w", encoding="utf-8") as f:
        json.dump(cost, f, ensure_ascii=False, indent=2)

    # ── summary.md ──
    md_path = os.path.join(output_dir, f"bird_{experiment}_{ts}_summary.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"# BIRD Mini-Dev Evaluation — {experiment}\n\n")
        f.write(f"**Date**: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"**Samples**: {n_total}\n\n")

        f.write("## Overall\n\n")
        f.write("| Config | EX | VES | Passed | Crashed | Avg Time | Avg Tokens | Tok/s | RAG Recall |\n")
        f.write("|--------|-----|-----|--------|---------|----------|------------|-------|------------|\n")
        for name in config_names:
            r = results[name]
            cr = r.get("crashed", 0)
            cr_str = f"⚠{cr}" if cr else "0"
            rr = r.get("avg_rag_recall")
            rr_str = f"{rr:.1%} ({r.get('rag_recall_samples', 0)})" if rr is not None else "-"
            f.write(f"| {name} | {r['ex_rate']:.1%} | {r['avg_ves']:.4f} | "
                    f"{r['passed']}/{r['total']} | {cr_str} | {r['avg_time_s']}s | "
                    f"{r['avg_tokens']} | {r['tokens_per_s']:.0f} | {rr_str} |\n")

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

        # ── Router distribution section ──
        has_router = any(results[n].get("router_traces") for n in config_names)
        if has_router:
            f.write("\n## Router Distribution\n\n")
            f.write("| Config | Router Simple | Router Complex | Simple EX | Complex EX |\n")
            f.write("|--------|---------------|----------------|-----------|------------|\n")
            for name in config_names:
                traces = results[name].get("router_traces", [])
                if traces:
                    s = sum(1 for t in traces if t.get("router_complexity") == "simple")
                    c = sum(1 for t in traces if t.get("router_complexity") == "complex")
                    s_pass = sum(1 for t in traces if t.get("router_complexity") == "simple" and t.get("ex_pass"))
                    c_pass = sum(1 for t in traces if t.get("router_complexity") == "complex" and t.get("ex_pass"))
                    s_ex = f"{s_pass/s:.1%}" if s else "-"
                    c_ex = f"{c_pass/c:.1%}" if c else "-"
                    f.write(f"| {name} | {s} | {c} | {s_ex} | {c_ex} |\n")

        # ── Pipeline module stats sections ──
        has_pipeline = any(results[n].get("pipeline_stats") for n in config_names)
        if has_pipeline:
            _write_pipeline_md(f, results, config_names)

        # ── Cost section ──
        if "test_run" in cost:
            tr = cost["test_run"]
            fp = cost["full_run_prediction"]
            f.write(f"\n## Token Cost\n\n")
            f.write(f"**Test run**: {tr['total_calls']} calls, {tr['total_tokens']:,} tokens, "
                    f"${tr['cost_total_usd']:.4f}\n\n")
            f.write(f"**Full run prediction** ({fp['total_calls']} calls): "
                    f"~{fp['estimated_tokens']:,} tokens, "
                    f"best ${fp['estimated_cost_usd_best']:.2f}, "
                    f"with retries ${fp['estimated_cost_usd_with_retries']:.2f}\n")
        elif "per_source" in cost:
            f.write(f"\n## Token Cost\n\n")
            for ks, c in cost["per_source"].items():
                if isinstance(c, dict):
                    tr = c.get("test_run", {})
                    fp = c.get("full_run_prediction", {})
                    if tr:
                        f.write(f"### {ks}\n")
                        f.write(f"- Test: {tr.get('total_calls')} calls, {tr.get('total_tokens',0):,} tokens, "
                                f"${tr.get('cost_total_usd',0):.4f}\n")
                    if fp:
                        f.write(f"- Predicted: {fp.get('total_calls')} calls, "
                                f"~{fp.get('estimated_tokens',0):,} tokens, "
                                f"best ${fp.get('estimated_cost_usd_best',0):.2f}\n")
                f.write("\n")

    return md_path


# ── CLI ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="BIRD Mini-Dev Evaluation")
    parser.add_argument("--test", action="store_true",
                        help="Small validation test (default: 10 samples, R0 only)")
    parser.add_argument("--samples", type=int, default=10,
                        help="Number of samples for test mode (default: 10)")
    parser.add_argument("--configs", type=str, default="R0",
                        help="Comma-separated config names for test (default: R0)")
    parser.add_argument("--exp", type=str, choices=["ablation"], default=None,
                        help="Full experiment: ablation (R0-R5)")
    parser.add_argument("--knowledge-source", type=str, default="rag",
                        choices=["none", "rag", "evidence"],
                        help="Knowledge source override for test mode (default: rag)")
    parser.add_argument("--max-workers", type=int, default=8,
                        help="Thread pool size (default: 8)")
    parser.add_argument("--progress-interval", type=int, default=60,
                        help="Progress print interval in seconds (default: 60)")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="Report output directory (default: reports/)")

    args = parser.parse_args()

    # Resolve output dir
    proj_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_dir = args.output_dir or os.path.join(proj_root, "reports")

    # Load samples
    all_samples = load_bird_dev()
    stats = get_stats(all_samples)
    print(f"Loaded {stats['total']} samples from {stats['databases']} databases")
    print(f"Difficulty: {stats['difficulty']}")

    timeout_ids_global = _load_previous_timeout_ids(output_dir)

    all_configs = _default_configs()
    config_map = {c["name"]: c for c in all_configs}

    def _resolve_configs(names_str: str) -> list[dict]:
        """Resolve config names, supporting short aliases (R0→R0_Baseline, etc.)."""
        result = []
        for name in (n.strip() for n in names_str.split(",")):
            if name in config_map:
                result.append(config_map[name])
            else:
                matches = [c for c in all_configs if c["name"].startswith(name + "_") or c["name"] == name]
                if len(matches) == 1:
                    result.append(matches[0])
                elif not matches:
                    print(f"Warning: unknown config '{name}', skipping")
                else:
                    print(f"Warning: ambiguous config '{name}' matches {[m['name'] for m in matches]}, using first")
                    result.append(matches[0])
        return result

    if not args.test and timeout_ids_global:
        # ── Full eval: prioritize previously-timed-out samples first ──
        all_samples.sort(key=lambda s: (
            0 if str(s.question_id) in timeout_ids_global else 1,
            {"simple": 0, "moderate": 1, "challenging": 2}[s.difficulty],
        ))
        pri_count = sum(1 for s in all_samples if str(s.question_id) in timeout_ids_global)
        print(f"[PRIORITY] {pri_count} previously-timed-out sample(s) run first")

    if args.test:
        # ── Small validation test ──
        import random
        random.seed(42)
        n = min(args.samples, len(all_samples))
        if n >= len(all_samples):
            # Full run → prioritize timeout-prone samples so they run first
            if timeout_ids_global:
                all_samples.sort(key=lambda s: (
                    0 if str(s.question_id) in timeout_ids_global else 1,
                    {"simple": 0, "moderate": 1, "challenging": 2}[s.difficulty],
                ))
                pri_count = sum(1 for s in all_samples if str(s.question_id) in timeout_ids_global)
                print(f"[PRIORITY] {pri_count} previously-timed-out sample(s) run first")
            test_samples = all_samples[:n]
        else:
            test_samples = random.sample(all_samples, n)

        test_configs = _resolve_configs(args.configs)
        if not test_configs:
            print(f"Error: no valid configs from {args.configs}")
            sys.exit(1)

        # Pre-init BIRD RAG collection in main thread (ChromaDB is not thread-safe for init)
        if args.knowledge_source == "rag":
            from nl2sql.rag_retrieve import get_bird_collection
            get_bird_collection()

        print(f"\n=== TEST MODE: {n} samples x {len(test_configs)} configs ===")
        print(f"Configs: {[c['name'] for c in test_configs]}")
        print(f"Knowledge source: {args.knowledge_source}")

        results = run_bird_eval(
            test_samples, test_configs,
            knowledge_source=args.knowledge_source,
            max_workers=args.max_workers,
            progress_interval=args.progress_interval,
        )
        cost = estimate_cost(results, len(all_samples))
        report_path = write_bird_report(results, cost, output_dir, "test")

        # ── Print cost estimate ──
        tr = cost["test_run"]
        fp = cost["full_run_prediction"]
        print(f"\n{'='*60}")
        print(f"=== Token Cost Estimate ===")
        print(f"Samples tested: {tr['samples']} × {tr['configs']} configs = {tr['total_calls']} calls")
        print(f"Total tokens: {tr['total_tokens']:,} (prompt: {tr['prompt_tokens']:,} / completion: {tr['completion_tokens']:,})")
        print(f"Avg tokens/sample: {cost['avg_tokens_per_sample']:,}")
        print(f"Test cost: ${tr['cost_total_usd']:.4f}")
        print(f"")
        print(f"--- Full Run Prediction ({fp['total_calls']} calls) ---")
        print(f"Estimated tokens: ~{fp['estimated_tokens']:,}")
        print(f"Estimated cost: ${fp['estimated_cost_usd_best']:.2f} (best) / ${fp['estimated_cost_usd_with_retries']:.2f} (with retries)")
        print(f"Report: {report_path}")

        # Force exit to work around non-daemon threads (SQLAlchemy pools,
        # ThreadPoolExecutor workers, ChromaDB) that block clean shutdown.
        import os as _os
        _os._exit(0)

    elif args.exp == "ablation":
        # Pre-init BIRD RAG collection in main thread (ChromaDB is not thread-safe for init)
        from nl2sql.rag_retrieve import get_bird_collection
        get_bird_collection()

        # ── Full R0-R5 ablation (all Full Graph) with checkpoint resume
        ckpt_path = os.path.join(output_dir, "checkpoint_ablation.json")
        ckpt = _load_checkpoint(ckpt_path)
        done_names = set(ckpt["results"].keys()) if ckpt else set()
        remaining = [c for c in all_configs if c["name"] not in done_names]

        if done_names:
            print(f"\n=== EXPERIMENT 1: Ablation Matrix R0-R5 (resume) ===")
            print(f"Already completed: {sorted(done_names)}")
            print(f"Remaining: {[c['name'] for c in remaining]}")
        else:
            print(f"\n=== EXPERIMENT 1: Ablation Matrix R0-R5 ===")

        if not remaining:
            print("All configs already completed!")
            results = ckpt["results"]
        else:
            results = run_bird_eval(
                all_samples, remaining,
                knowledge_source="rag",
                max_workers=args.max_workers,
                progress_interval=args.progress_interval,
                checkpoint_path=ckpt_path,
            )
            # Merge with existing checkpoint results
            if ckpt:
                for k, v in ckpt["results"].items():
                    results[k] = v

        cost = estimate_cost(results, len(all_samples))
        report_path = write_bird_report(results, cost, output_dir, "ablation")
        print(f"\nReport: {report_path}")

        # Force exit to work around non-daemon threads (SQLAlchemy pools,
        # ThreadPoolExecutor workers) that may block clean shutdown.
        import os as _os
        _os._exit(0)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
