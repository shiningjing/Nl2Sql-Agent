"""Quick #701 timing test."""
import sys, os, json, time, warnings
warnings.filterwarnings("ignore")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv; load_dotenv()

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from evaluation.bird_loader import load_bird_dev, get_database_url
from evaluation.metrics import exec_match
from agent.graphs.full_graph import create_full_graph

samples = load_bird_dev()
s = next(x for x in samples if x.question_id == "701")
db_url = get_database_url(s)

cache_dir = os.path.join("reports", ".gold_cache")
gold_cache_map = {}
for fname in os.listdir(cache_dir):
    if fname.endswith(".json"):
        with open(os.path.join(cache_dir, fname), encoding="utf-8") as f:
            gold_cache_map.update(json.load(f))
gold_entry = gold_cache_map.get("701")

print(f"#701 [{s.db_id}]")
print(f"Q: {s.question[:100]}")

# Phase 1: graph with timeout
def run_graph():
    graph = create_full_graph()
    return graph.invoke({
        "question": s.question, "db_id": s.db_id,
        "rag_schema": False, "rag_domain": False,
        "skip_schema": False, "sample_rows": False,
        "multi_candidate": False, "fewshot_enabled": False,
        "decomposer_enabled": True, "database_url": db_url,
    })

pool = ThreadPoolExecutor(max_workers=1)
try:
    print("graph.invoke() starting...", flush=True)
    t0 = time.time()
    fut = pool.submit(run_graph)
    state = fut.result(timeout=120)
    graph_s = round(time.time() - t0, 1)
    gen_sql = state.get("sql", "")
    print(f"graph done: {graph_s}s", flush=True)
    print(f"SQL len: {len(gen_sql)}", flush=True)
    print(f"sem_pass: {state.get('semantic_pass')}, retry: {state.get('retry_count')}", flush=True)
except FutureTimeoutError:
    print("GRAPH TIMEOUT after 120s!", flush=True)
    gen_sql = ""
    state = {}
    graph_s = 120
finally:
    pool.shutdown(wait=False)

# Phase 2: exec_match
if gen_sql:
    print("exec_match starting...", flush=True)
    t0 = time.time()
    ex_info = exec_match(gen_sql, s.gold_sql, database_url=db_url, gold_cache=gold_entry)
    exec_s = round(time.time() - t0, 4)
    print(f"exec_match done: {exec_s}s", flush=True)
    print(f"EX={ex_info['ex']} | {ex_info['detail']}", flush=True)
    print(f"gold_time_ms={ex_info['gold_time_ms']:.0f} gen_time_ms={ex_info['gen_time_ms']:.0f}", flush=True)
else:
    exec_s = 0
    print("No SQL, skipping exec_match", flush=True)

total_s = round(graph_s + exec_s, 1)
print(f"Total: graph={graph_s}s + exec_match={exec_s}s = {total_s}s", flush=True)
