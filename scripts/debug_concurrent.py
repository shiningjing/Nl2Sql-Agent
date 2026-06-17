"""Concurrency stress test: 8 workers on same DB, per-phase timing to detect bottlenecks.

Runs codebase_community samples with max_workers=8, logging every phase transition
so we can distinguish: API rate-limit vs schema reflection vs SQL execution vs LLM hang.
"""
import os
import sys
import threading
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

warnings.filterwarnings("ignore", message=".*allowed_objects.*")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from evaluation.bird_loader import load_bird_dev, get_database_url
from agent.graphs.full_graph import create_full_graph

TARGET_IDS = ["701", "595", "637", "531", "586", "634", "639", "547"]
MAX_WORKERS = 8
PER_SAMPLE_TIMEOUT = 600

_print_lock = threading.Lock()

def ts() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]

def log(worker_id, msg):
    with _print_lock:
        print(f"[{ts()}] W{worker_id} {msg}", flush=True)

def run_one(worker_id, sample, graph):
    db_url = get_database_url(sample)
    qid = sample.question_id
    log(worker_id, f"#{qid} START  diff={sample.difficulty}  Q: {sample.question[:60]}")

    initial_state = {
        "question": sample.question,
        "rag_schema": True,
        "rag_domain": True,
        "multi_candidate": True,
        "rag_k": 8,
        "rag_column_prune": False,
        "fewshot_enabled": True,
        "database_url": db_url,
        "db_id": sample.db_id,
    }

    t_total_start = time.time()
    log(worker_id, f"#{qid} PHASE invoke START")

    try:
        result = graph.invoke(initial_state)
        elapsed_total = round(time.time() - t_total_start, 2)
        log(worker_id, f"#{qid} PHASE invoke DONE  {elapsed_total}s")

        sql = result.get("sql", "")
        exec_result = result.get("exec_result", {})
        token_usage = result.get("token_usage", {})
        trace_events = result.get("trace_events", [])

        # Extract per-node timing from trace events
        node_times = {}
        node_enter_ts = {}
        for e in trace_events:
            if e.get("event") == "node_enter":
                node_enter_ts[e["node"]] = e.get("_ts", 0)
            elif e.get("event") == "node_exit":
                node = e["node"]
                enter_ts = node_enter_ts.get(node, 0)
                exit_ts = e.get("_ts", 0)
                if enter_ts and exit_ts:
                    node_times[node] = round(exit_ts - enter_ts, 2)

        log(worker_id, f"#{qid} RESULT  total={elapsed_total}s  tokens={token_usage.get('total',0)}  "
                       f"exec_ok={exec_result.get('success',False)}  nodes={list(node_times.keys())}  "
                       f"node_times={node_times}")

        return {
            "qid": qid,
            "success": True,
            "elapsed": elapsed_total,
            "tokens": token_usage.get("total", 0),
            "exec_ok": exec_result.get("success", False),
            "node_times": node_times,
            "sql": sql[:120] if sql else "",
            "error": (exec_result.get("error") or "") if not exec_result.get("success") else "",
        }

    except Exception as e:
        elapsed_total = round(time.time() - t_total_start, 2)
        log(worker_id, f"#{qid} FAILED  after {elapsed_total}s  {type(e).__name__}: {str(e)[:200]}")
        return {
            "qid": qid,
            "success": False,
            "elapsed": elapsed_total,
            "error_type": type(e).__name__,
            "error_msg": str(e)[:300],
        }

def main():
    samples = load_bird_dev()
    by_id = {s.question_id: s for s in samples}

    print(f"{'='*70}")
    print(f"Concurrency Stress Test")
    print(f"  DB: codebase_community (459MB)")
    print(f"  Workers: {MAX_WORKERS}")
    print(f"  Targets: {TARGET_IDS}")
    print(f"  Per-sample timeout: {PER_SAMPLE_TIMEOUT}s")
    print(f"{'='*70}")

    # Pre-create graph (shared across workers — tests concurrency safety)
    t0 = time.time()
    graph = create_full_graph()
    print(f"Graph pre-created in {time.time()-t0:.1f}s")
    print(f"Starting {len(TARGET_IDS)} samples with {MAX_WORKERS} workers...\n")

    t_total = time.time()
    results = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {}
        for i, qid in enumerate(TARGET_IDS):
            sample = by_id[qid]
            if i > 0:
                time.sleep(0.3)  # stagger submissions slightly for log readability
            futures[pool.submit(run_one, i, sample, graph)] = qid

        for future in as_completed(futures):
            qid = futures[future]
            try:
                r = future.result(timeout=PER_SAMPLE_TIMEOUT)
                results[qid] = r
            except Exception as e:
                print(f"[{ts()}] #{qid} HARD TIMEOUT after {PER_SAMPLE_TIMEOUT}s: {e}")
                results[qid] = {"qid": qid, "success": False, "error_type": "hard_timeout"}

    total_elapsed = time.time() - t_total

    print(f"\n{'='*70}")
    print(f"SUMMARY  total_parallel_time={total_elapsed:.1f}s  workers={MAX_WORKERS}")
    print(f"{'='*70}")
    for qid in TARGET_IDS:
        r = results.get(qid, {})
        ok = r.get("success", False)
        ela = r.get("elapsed", "?")
        tok = r.get("tokens", "?")
        err = r.get("error", "") or r.get("error_msg", "")
        node_t = r.get("node_times", {})
        status = "OK" if ok else f"FAIL({r.get('error_type','?')})"
        print(f"  #{qid:>4s}  {status:20s}  {str(ela)+'s':>8s}  {str(tok):>6s} tok  nodes={node_t}")
        if err:
            print(f"         error: {err[:150]}")
        if r.get("sql"):
            print(f"         sql: {r['sql']}")

    # Diagnostic: if all samples have similar elapsed time but some are much slower,
    # it's API rate-limiting. If one sample is way slower, it's that sample's query.
    ok_times = [r["elapsed"] for r in results.values() if r.get("success")]
    if len(ok_times) >= 2:
        avg = sum(ok_times) / len(ok_times)
        mx = max(ok_times)
        print(f"\n  Timing analysis: avg={avg:.1f}s  max={mx:.1f}s  spread={mx-min(ok_times):.1f}s")
        if mx > avg * 3:
            print(f"  >>> LARGE SPREAD: max {mx:.1f}s >> avg {avg:.1f}s suggests per-sample bottleneck, not API limit")
        else:
            print(f"  Moderate spread: consistent with normal API latency variance")

if __name__ == "__main__":
    main()
