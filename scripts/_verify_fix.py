"""Quick 20-sample test with FIXED exec_match arg order."""
import sys, os, json, random, warnings, time
warnings.filterwarnings("ignore")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv; load_dotenv()

from evaluation.bird_loader import load_bird_dev, get_database_url
from evaluation.metrics import exec_match
from agent.graphs.full_graph import create_full_graph
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError

random.seed(123)
samples = load_bird_dev()
random.shuffle(samples)
test = samples[:20]

cache_dir = os.path.join("reports", ".gold_cache")
gold_cache_map = {}
for fname in os.listdir(cache_dir):
    if fname.endswith(".json"):
        with open(os.path.join(cache_dir, fname), encoding="utf-8") as f:
            gold_cache_map.update(json.load(f))

ok = 0
fail = 0
timeout = 0
details = []

for i, s in enumerate(test):
    qid = s.question_id
    db_url = get_database_url(s)
    gold_entry = gold_cache_map.get(qid)

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
    gen_sql = ""
    try:
        t0 = time.time()
        fut = pool.submit(run_graph)
        state = fut.result(timeout=120)
        gen_sql = state.get("sql", "")
        graph_s = round(time.time() - t0, 1)
    except FutureTimeoutError:
        graph_s = 120
    finally:
        pool.shutdown(wait=False)

    if gen_sql and gold_entry:
        # FIXED: gold_sql first, gen_sql second
        ex_info = exec_match(s.gold_sql, gen_sql, database_url=db_url,
                             gold_cache=gold_entry)
        if ex_info["ex"]:
            ok += 1
        elif "timed out" in ex_info["detail"]:
            timeout += 1
            details.append((qid, "TIMEOUT", ex_info["detail"][:80]))
        else:
            fail += 1
            details.append((qid, "FAIL", ex_info["detail"][:80]))
    elif not gold_entry:
        details.append((qid, "NO_CACHE", "gold cache missing"))

    print(f"  {i+1}/{len(test)} #{qid} [{s.db_id}] graph={graph_s}s EX={ex_info.get('ex', '?')}", flush=True)

print(f"\n=== 20-sample FIXED result ===")
print(f"OK={ok} FAIL={fail} TIMEOUT={timeout}")
for d in details:
    print(f"  #{d[0]}: {d[1]} — {d[2]}")
print(f"EX = {ok}/{ok+fail+timeout} = {ok/(ok+fail+timeout)*100:.1f}%")
