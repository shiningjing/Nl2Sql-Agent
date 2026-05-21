"""Diagnose timeout root cause: per-step wall-clock timing for timeout-prone samples.

Tests both sequentially and concurrently to identify where time is spent
and why specific questions (Q701, Q595, Q518, Q1505) always timeout in eval.
"""
import json
import os
import sys
import time

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)
os.chdir(_PROJECT_ROOT)

from src.eval.bird_loader import load_bird_dev, get_database_url
from src.agent.graphs.full_graph import create_full_graph
from src.agent.state import AgentState
from src.obs.logger import TraceLogger

# ── Timeout-prone samples ──
TIMEOUT_IDS = {"518", "595", "701", "1505"}
# Add a few control samples from small DBs
CONTROL_IDS = {"26", "201", "281", "371"}

samples = load_bird_dev()
target = [s for s in samples if s.question_id in TIMEOUT_IDS | CONTROL_IDS]
print(f"Loaded {len(target)} samples")
for s in target:
    db_path = get_database_url(s).replace("sqlite:///", "")
    size_mb = os.path.getsize(db_path) / (1024 * 1024) if os.path.exists(db_path) else 0
    tag = "TIMEOUT-PRONE" if s.question_id in TIMEOUT_IDS else "control"
    print(f"  Q{s.question_id} [{tag}] db={s.db_id} ({size_mb:.0f}MB) diff={s.difficulty}")

config = {
    "rag_schema": True, "rag_domain": True, "multi_candidate": True,
    "rag_column_prune": True, "fewshot_enabled": True,
    "sample_rows": True, "k": 8,
}


def run_with_substep_timing(sample, label=""):
    """Run a single sample with detailed per-step timing using TraceLogger events."""
    database_url = get_database_url(sample)
    t0_total = time.time()

    # Step 1: Graph creation
    t0 = time.time()
    graph = create_full_graph()
    t_graph = time.time() - t0

    # Step 2: Graph invoke
    t0 = time.time()
    try:
        state = graph.invoke({
            "question": sample.question,
            "db_id": sample.db_id,
            "rag_schema": config["rag_schema"],
            "rag_domain": config["rag_domain"],
            "multi_candidate": config["multi_candidate"],
            "rag_k": config["k"],
            "rag_column_prune": config["rag_column_prune"],
            "fewshot_enabled": config["fewshot_enabled"],
            "database_url": database_url,
            "_domain_notes_override": "",
        })
    except Exception as e:
        t_invoke = time.time() - t0
        return {
            "question_id": sample.question_id,
            "db_id": sample.db_id,
            "ok": False,
            "error": f"{type(e).__name__}: {str(e)[:200]}",
            "t_total": time.time() - t0_total,
            "t_graph": t_graph,
            "t_invoke": t_invoke,
        }

    t_invoke = time.time() - t0
    t_total = time.time() - t0_total

    # Extract per-node timings from trace events
    tlog = state.get("tlog")
    node_times = {}
    if tlog:
        # Build per-node timing from events
        events = list(tlog.events)
        node_entries = {}
        for e in events:
            if e.get("event") == "node_enter":
                node_entries[e["node"]] = e.get("ts", "")
            elif e.get("event") == "node_exit":
                dur = e.get("duration_s", 0)
                node_times[e["node"]] = dur

    return {
        "question_id": sample.question_id,
        "db_id": sample.db_id,
        "ok": True,
        "t_total": round(t_total, 3),
        "t_graph": round(t_graph, 4),
        "t_invoke": round(t_invoke, 3),
        "node_times": node_times,
        "complexity": state.get("complexity"),
        "router_method": state.get("router_method"),
    }


print("\n" + "=" * 60)
print("PHASE 1: Sequential per-step timing")
print("=" * 60)

for s in target:
    r = run_with_substep_timing(s)
    status = "OK" if r["ok"] else f"FAIL: {r.get('error', 'unknown')}"
    tag = "⚠ TIMEOUT-PRONE" if s.question_id in TIMEOUT_IDS else "  control"
    print(f"\n[{tag}] Q{s.question_id} db={s.db_id} diff={s.difficulty}")
    print(f"  Status: {status}")
    if r["ok"]:
        print(f"  Total: {r['t_total']}s | Graph: {r['t_graph']}s | Invoke: {r['t_invoke']}s")
        print(f"  Complexity: {r['complexity']} | Router: {r['router_method']}")
        nodes = r["node_times"]
        total_node = sum(nodes.values())
        print(f"  Node times (sum={total_node:.2f}s):")
        for node_name, dur in nodes.items():
            bar = "█" * int(dur / 0.5)
            print(f"    {node_name:25s}: {dur:7.3f}s {bar}")

print("\n" + "=" * 60)
print("PHASE 2: ThreadPoolExecutor(max_workers=4) timing breakdown")
print("=" * 60)

from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Use a lock to serialize print output from threads
print_lock = threading.Lock()


def run_with_thread_timing(sample):
    """Run with per-thread timing, capturing start/end times for each major step."""
    tid = threading.current_thread().name
    database_url = get_database_url(sample)
    steps = {}

    t0 = time.perf_counter()
    try:
        graph = create_full_graph()
        steps["graph_create"] = round(time.perf_counter() - t0, 4)

        t0 = time.perf_counter()
        state = graph.invoke({
            "question": sample.question,
            "db_id": sample.db_id,
            "rag_schema": config["rag_schema"],
            "rag_domain": config["rag_domain"],
            "multi_candidate": config["multi_candidate"],
            "rag_k": config["k"],
            "rag_column_prune": config["rag_column_prune"],
            "fewshot_enabled": config["fewshot_enabled"],
            "database_url": database_url,
            "_domain_notes_override": "",
        })
        steps["graph_invoke"] = round(time.perf_counter() - t0, 4)

        tlog = state.get("tlog")
        node_times = {}
        if tlog:
            for e in tlog.events:
                if e.get("event") == "node_exit":
                    node_times[e["node"]] = e.get("duration_s", 0)

        steps["total"] = sum(steps.values())
        return {
            "question_id": sample.question_id,
            "db_id": sample.db_id,
            "ok": True,
            "thread": tid,
            "steps": steps,
            "node_times": node_times,
            "complexity": state.get("complexity"),
        }
    except Exception as e:
        steps["total"] = sum(steps.values())
        import traceback
        return {
            "question_id": sample.question_id,
            "db_id": sample.db_id,
            "ok": False,
            "thread": tid,
            "steps": steps,
            "error": f"{type(e).__name__}: {str(e)[:200]}",
            "traceback": traceback.format_exc()[-500:],
        }


# Run all target samples concurrently with 4 workers
results = []
t0_batch = time.perf_counter()
with ThreadPoolExecutor(max_workers=4) as pool:
    futures = {pool.submit(run_with_thread_timing, s): s for s in target}
    for f in as_completed(futures):
        r = f.result()
        results.append(r)
        s = futures[f]
        tag = "⚠" if s.question_id in TIMEOUT_IDS else " "
        status = "OK" if r["ok"] else f"FAIL"
        elapsed = r["steps"].get("total", 0)
        print(f"  [{tag}] Q{s.question_id} db={s.db_id} tid={r['thread']} {status} total={elapsed:.1f}s")
t_batch = time.perf_counter() - t0_batch

results.sort(key=lambda r: r["question_id"])
print(f"\n  Batch wall-clock: {t_batch:.1f}s")

print("\n  Per-question breakdown:")
for r in results:
    tag = "⚠" if r["question_id"] in TIMEOUT_IDS else " "
    if r["ok"]:
        steps = r["steps"]
        nodes = r["node_times"]
        print(f"  [{tag}] Q{r['question_id']} {r['db_id']} ({r['complexity']}):")
        print(f"      graph={steps.get('graph_create', 0):.3f}s invoke={steps.get('graph_invoke', 0):.1f}s")
        for n, d in sorted(nodes.items(), key=lambda x: -x[1]):
            print(f"        {n}: {d:.2f}s")
    else:
        print(f"  [{tag}] Q{r['question_id']} {r['db_id']}: FAIL {r.get('error', '')[:120]}")

# ── Summary ──
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
timeout_results = [r for r in results if r["question_id"] in TIMEOUT_IDS]
control_results = [r for r in results if r["question_id"] not in TIMEOUT_IDS]

for label, group in [("Timeout-prone", timeout_results), ("Control", control_results)]:
    ok = [r for r in group if r["ok"]]
    fail = [r for r in group if not r["ok"]]
    avg_total = sum(r["steps"].get("total", 0) for r in ok) / len(ok) if ok else 0
    avg_invoke = sum(r["steps"].get("graph_invoke", 0) for r in ok) / len(ok) if ok else 0
    print(f"\n{label} ({len(group)} samples):")
    print(f"  OK: {len(ok)}, FAIL: {len(fail)}")
    if ok:
        print(f"  Avg total: {avg_total:.1f}s, Avg invoke: {avg_invoke:.1f}s")
        # Show longest node
        all_nodes = {}
        for r in ok:
            for n, d in r.get("node_times", {}).items():
                if n not in all_nodes:
                    all_nodes[n] = []
                all_nodes[n].append(d)
        if all_nodes:
            print("  Avg per-node time:")
            for n, times in sorted(all_nodes.items(), key=lambda x: -sum(x[1]) / len(x[1])):
                print(f"    {n}: {sum(times)/len(times):.2f}s avg, {max(times):.2f}s max")
    if fail:
        for r in fail:
            print(f"  FAIL Q{r['question_id']}: {r.get('error', '')[:150]}")
