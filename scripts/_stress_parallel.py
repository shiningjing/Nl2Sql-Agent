"""Parallel stress test — isolate graph vs LLM vs exec_match timing.

Usage:
  python scripts/_stress_parallel.py [--db codebase_community] [--workers 8] [--samples 16]
"""
import argparse, io, json, os, sys, time, threading

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from datetime import datetime
from evaluation.bird_loader import load_bird_dev, get_database_url
from evaluation.metrics import exec_match
from agent.graphs.full_graph import create_full_graph
from observability.logger import TraceLogger


# ── Per-sample runner with split timing ──────────────────────────────────────

def run_one(sample, config: dict, log_dir: str,
             gold_cache_map: dict[str, dict] | None = None) -> dict:
    """Run evaluate_bird_sample-equivalent with fine-grained timing."""
    database_url = get_database_url(sample)

    domain_override = _get_domain_override(sample, config.get("knowledge_source", "rag"))

    tlog = TraceLogger(log_dir=log_dir, trace_id=None)

    # ── Phase 1: Graph ──
    t0 = time.time()
    graph_error = ""
    last_graph_node = "?"
    try:
        graph = create_full_graph()
        state = graph.invoke({
            "question": sample.question,
            "db_id": sample.db_id,
            "rag_schema": config.get("rag_schema", False),
            "rag_domain": config.get("rag_domain", False),
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
        for ev in reversed(tlog.events):
            if ev["event"] in ("node_exit", "node_enter"):
                last_graph_node = ev.get("node", "?")
                break
    except Exception as e:
        gen_sql = ""
        state = {}
        graph_error = f"{type(e).__name__}: {str(e)[:150]}"
        for ev in reversed(tlog.events):
            if ev["event"] == "node_enter":
                last_graph_node = ev.get("node", "?")
                break
    t1 = time.time()

    # ── Phase 2: exec_match ──
    if gen_sql:
        gold_entry = gold_cache_map.get(sample.question_id) if gold_cache_map else None
        ex_info = exec_match(gen_sql, sample.gold_sql, database_url=database_url,
                             gold_cache=gold_entry)
    else:
        err_detail = graph_error or "SQL generation failed"
        ex_info = {"ex": False, "detail": err_detail,
                   "gold_time_ms": 0, "gen_time_ms": 0, "gold_rows": 0, "gen_rows": 0}
    t2 = time.time()

    graph_s = round(t1 - t0, 3)
    exec_s = round(t2 - t1, 4)
    total_s = round(t2 - t0, 2)

    return {
        "question_id": sample.question_id,
        "db_id": sample.db_id,
        "difficulty": sample.difficulty,
        "ex": ex_info["ex"],
        "detail": ex_info["detail"][:100],
        "graph_s": graph_s,
        "exec_s": exec_s,
        "total_s": total_s,
        "last_graph_node": last_graph_node,
        "trace_id": tlog.trace_id,
        "token_usage": state.get("token_usage", {}),
    }


def _get_domain_override(sample, knowledge_source: str) -> str | None:
    """Return evidence text or None, mirroring eval_bird._get_domain_override."""
    if knowledge_source == "evidence":
        return getattr(sample, "evidence", "") or ""
    return None


# ── Monitor thread ───────────────────────────────────────────────────────────

def _monitor(pending: dict, stop_event: threading.Event, interval: float = 3.0):
    """Print active thread states every `interval` seconds."""
    while not stop_event.is_set():
        stop_event.wait(interval)
        if stop_event.is_set():
            break
        now = time.time()
        active = []
        for fut, (qid, db, t_start) in list(pending.items()):
            if not fut.done():
                elapsed = now - t_start
                active.append((qid, db, elapsed))
        if active:
            active.sort(key=lambda x: -x[2])
            status = " | ".join(f"#{q}({db[:12]}:{e:.0f}s)" for q, db, e in active[:6])
            print(f"  [MONITOR] {len(active)} active: {status}", flush=True)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=str, default="codebase_community",
                        help="Target database (default: codebase_community)")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--samples", type=int, default=16,
                        help="Number of samples (default: 16)")
    parser.add_argument("--timeout", type=int, default=90,
                        help="Per-sample hard timeout in seconds (default: 90)")
    parser.add_argument("--include-slow-dbs", action="store_true",
                        help="Also include samples from mimic_iv / european_football_2")
    args = parser.parse_args()

    # ── Load samples ──
    all_samples = load_bird_dev()
    by_db: dict[str, list] = defaultdict(list)
    for s in all_samples:
        by_db[s.db_id].append(s)

    target = args.db
    test_samples = by_db.get(target, [])[:args.samples]
    if args.include_slow_dbs:
        for db in ["mimic_iv", "european_football_2", "thrombosis_prediction"]:
            extra = by_db.get(db, [])[:4]
            test_samples.extend(extra)

    if not test_samples:
        print(f"No samples found for db={target}")
        print(f"Available: {list(by_db.keys())}")
        return

    # Ensure #701 is included if testing codebase_community
    if args.db == "codebase_community":
        s701 = next((s for s in all_samples if s.question_id == "701"), None)
        if s701 and s701 not in test_samples:
            test_samples.insert(0, s701)

    print(f"=== Parallel Stress Test ===")
    print(f"Target: {args.db} (+{[''] if not args.include_slow_dbs else 'mimic_iv/etc'})")
    print(f"Samples: {len(test_samples)} | Workers: {args.workers} | Timeout: {args.timeout}s")
    print(f"IDs: {[s.question_id for s in test_samples]}")

    # R0_Baseline config (simplest path)
    config = {
        "rag_schema": False, "rag_domain": False, "sample_rows": False,
        "multi_candidate": False, "fewshot_enabled": False,
        "decomposer_enabled": False, "use_full_graph": True,
        "skip_schema": False,
    }

    log_dir = os.path.join("logs", "traces", f"stress_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    os.makedirs(log_dir, exist_ok=True)
    print(f"Trace dir: {log_dir}")
    print()

    # ── Load gold result cache ──
    gold_cache_map: dict[str, dict] = {}
    _gold_cache_dir = os.path.join("reports", ".gold_cache")
    if os.path.isdir(_gold_cache_dir):
        for fname in os.listdir(_gold_cache_dir):
            if fname.endswith(".json"):
                with open(os.path.join(_gold_cache_dir, fname), "r", encoding="utf-8") as f:
                    gold_cache_map.update(json.load(f))
        if gold_cache_map:
            n_ok = sum(1 for v in gold_cache_map.values() if v.get("norm") is not None)
            print(f"Gold cache: {len(gold_cache_map)} entries ({n_ok} ok)")

    # ── Run ──
    stop_monitor = threading.Event()
    pending: dict = {}
    results: list[dict] = []
    crashes: list[dict] = []

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        # Submit all
        for s in test_samples:
            fut = pool.submit(run_one, s, config, log_dir, gold_cache_map)
            pending[fut] = (s.question_id, s.db_id, time.time())

        monitor_t = threading.Thread(
            target=_monitor, args=(pending, stop_monitor), daemon=True)
        monitor_t.start()

        t_batch_start = time.time()
        for fut in as_completed(pending, timeout=args.timeout * 2):
            qid, db, t_start = pending.pop(fut)
            try:
                r = fut.result(timeout=5)
                results.append(r)
                status = "PASS" if r["ex"] else "FAIL"
                print(f"  [#{r['question_id']:<5} {r['db_id']:<22} {status:<5}"
                      f" graph={r['graph_s']:.1f}s exec={r['exec_s']:.4f}s"
                      f" total={r['total_s']:.1f}s"
                      f" last={r['last_graph_node']:<18}"
                      f" | {r['detail'][:60]}")
            except Exception as e:
                crashes.append({"question_id": qid, "db_id": db,
                                "error": f"{type(e).__name__}: {str(e)[:150]}"})
                print(f"  [#{qid:<5} {db:<22} CRASH | {crashes[-1]['error']}")

            # Stop early if all done
            if len(results) + len(crashes) >= len(test_samples):
                break

        stop_monitor.set()
        monitor_t.join(timeout=2)

    # ── Summary ──
    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)

    graph_times = [r["graph_s"] for r in results]
    exec_times = [r["exec_s"] for r in results]
    total_times = [r["total_s"] for r in results]

    print(f"Results: {len(results)} completed, {len(crashes)} crashed")
    print(f"Timing breakdown (avg):")
    print(f"  Graph:      {sum(graph_times)/len(graph_times):.2f}s")
    print(f"  exec_match: {sum(exec_times)/len(exec_times):.4f}s")
    print(f"  Total:      {sum(total_times)/len(total_times):.2f}s")

    # Timing distribution
    print()
    print("Slowest 5 (by graph time):")
    for r in sorted(results, key=lambda r: -r["graph_s"])[:5]:
        print(f"  #{r['question_id']:<5} graph={r['graph_s']:.1f}s"
              f" exec={r['exec_s']:.4f}s total={r['total_s']:.1f}s"
              f" last_node={r['last_graph_node']}")

    print()
    print("Slowest 5 (by exec_match time):")
    for r in sorted(results, key=lambda r: -r["exec_s"])[:5]:
        print(f"  #{r['question_id']:<5} graph={r['graph_s']:.1f}s"
              f" exec={r['exec_s']:.4f}s total={r['total_s']:.1f}s"
              f" | {r['detail']}")

    # Stuck detection
    stuck_candidates = [r for r in results if r["graph_s"] > 30]
    if stuck_candidates:
        print()
        print(f"WARNING: {len(stuck_candidates)} samples had graph > 30s (possible LLM hang):")
        for r in stuck_candidates:
            print(f"  #{r['question_id']} graph={r['graph_s']:.1f}s last_node={r['last_graph_node']}")

    dead_candidates = [r for r in results if r["exec_s"] > 10]
    if dead_candidates:
        print()
        print(f"WARNING: {len(dead_candidates)} samples had exec_match > 10s (possible slow SQL):")
        for r in dead_candidates:
            print(f"  #{r['question_id']} exec={r['exec_s']:.2f}s | {r['detail']}")

    if crashes:
        print()
        print(f"Crashes ({len(crashes)}):")
        for c in crashes:
            print(f"  #{c['question_id']} [{c['db_id']}]: {c['error']}")

    # Orphan traces
    print()
    print("Orphan traces (node_enter without node_exit):")
    orphans = 0
    for fname in sorted(os.listdir(log_dir)):
        if fname.endswith(".jsonl"):
            path = os.path.join(log_dir, fname)
            with open(path, encoding="utf-8") as fh:
                lines = [json.loads(l) for l in fh if l.strip()]
            enters = {e.get("node") for e in lines if e.get("event") == "node_enter"}
            exits = {e.get("node") for e in lines if e.get("event") == "node_exit"}
            stuck = enters - exits
            if stuck:
                orphans += 1
                print(f"  {fname}: stuck at {stuck} ({len(lines)} events)")
    if orphans == 0:
        print("  (none)")


if __name__ == "__main__":
    main()
