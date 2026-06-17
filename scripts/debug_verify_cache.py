"""Quick verification: engine & schema cache for timeout-prone databases."""
import os
import sys
import time

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)
os.chdir(_PROJECT_ROOT)

from evaluation.bird_loader import load_bird_dev, get_database_url
from agent.graphs.full_graph import create_full_graph

samples = load_bird_dev()

config = {
    "rag_schema": True, "rag_domain": True, "multi_candidate": True,
    "rag_column_prune": True, "fewshot_enabled": True,
    "sample_rows": True, "k": 8,
}

for qid in ["595", "701", "518", "1505"]:
    s = [s for s in samples if s.question_id == qid][0]
    db_url = get_database_url(s)
    db_path = db_url.replace("sqlite:///", "")
    size_mb = os.path.getsize(db_path) / (1024 * 1024) if os.path.exists(db_path) else 0

    print(f"\nQ{qid} {s.db_id} [{s.difficulty}] ({size_mb:.0f}MB)")
    print(f"  Question: {s.question[:80]}...")

    t0 = time.time()
    try:
        graph = create_full_graph()
        state = graph.invoke({
            "question": s.question,
            "db_id": s.db_id,
            **config,
            "database_url": db_url,
            "_domain_notes_override": "",
        })
        elapsed = time.time() - t0
        gen_sql = state.get("sql", "")
        print(f"  Time: {elapsed:.1f}s | SQL: {len(gen_sql)} chars | complexity={state.get('complexity')} | router={state.get('router_method')}")
    except Exception as e:
        elapsed = time.time() - t0
        print(f"  FAIL after {elapsed:.1f}s: {type(e).__name__}: {str(e)[:150]}")

print("\nDone.")
