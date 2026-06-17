"""#701 diagnosis — graph gen SQL + execution time."""
import sys, os, json, time, warnings
warnings.filterwarnings("ignore")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv; load_dotenv()

from sqlalchemy import create_engine, text
from evaluation.bird_loader import load_bird_dev, get_database_url
from agent.graphs.full_graph import create_full_graph
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError

s = next(x for x in load_bird_dev() if x.question_id == "701")
db_url = get_database_url(s)

print(f"#701 [{s.db_id}]")
print(f"Q: {s.question}")
print()

# ──── Gold SQL timing ────
print("=== Gold SQL ===")
print(s.gold_sql[:300])

def run_gold():
    eng = create_engine(db_url)
    with eng.connect() as conn:
        conn.execute(text("PRAGMA journal_mode=WAL"))
        return conn.execute(text(s.gold_sql)).fetchall()

t0 = time.time()
pool = ThreadPoolExecutor(max_workers=1)
try:
    fut = pool.submit(run_gold)
    rows = fut.result(timeout=30)
    gold_s = round(time.time() - t0, 2)
    print(f"  Time: {gold_s}s, rows: {len(rows)}")
except FutureTimeoutError:
    gold_s = 30
    print(f"  TIMEOUT after 30s")
finally:
    pool.shutdown(wait=False)
print()

# ──── Graph invoke ────
print("=== Graph invoke ===")
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
    graph_s = round(time.time() - t0, 1)
    gen_sql = state.get("sql", "")
    print(f"  Time: {graph_s}s")
    print(f"  SQL ({len(gen_sql)} chars):")
    print(f"  {gen_sql[:500]}")
except FutureTimeoutError:
    graph_s = 120
    print(f"  GRAPH TIMEOUT after 120s")
finally:
    pool.shutdown(wait=False)
print()

# ──── Generated SQL timing ────
if gen_sql:
    print("=== Generated SQL execution ===")

    def run_gen():
        eng = create_engine(db_url)
        with eng.connect() as conn:
            conn.execute(text("PRAGMA journal_mode=WAL"))
            return conn.execute(text(gen_sql)).fetchall()

    t0 = time.time()
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        fut = pool.submit(run_gen)
        rows = fut.result(timeout=60)
        gen_exec_s = round(time.time() - t0, 2)
        print(f"  Time: {gen_exec_s}s, rows: {len(rows)}")
    except FutureTimeoutError:
        gen_exec_s = 60
        print(f"  TIMEOUT after 60s")
    finally:
        pool.shutdown(wait=False)

    print()
    print(f"Summary: graph={graph_s}s, gold_exec={gold_s}s, gen_exec={gen_exec_s}s")
