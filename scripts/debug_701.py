"""Debug script: run specific codebase_community samples with long timeout."""
import json
import os
import sys
import time
import warnings
warnings.filterwarnings("ignore", message=".*allowed_objects.*")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.eval.bird_loader import load_bird_dev, get_database_url
from src.agent.graphs.full_graph import create_full_graph
from nl2sql.config import Config

# Pick 5 codebase_community samples
TARGET_IDS = ["701"]
TIMEOUT_S = 600  # 10 minutes per sample

def run_sample(sample):
    """Run one sample with R5 config, return result."""
    db_url = get_database_url(sample)

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

    print(f"\n{'='*60}")
    print(f"#{sample.question_id}  diff={sample.difficulty}")
    print(f"  Q: {sample.question[:100]}")
    print(f"  DB: {db_url}")
    print(f"  Gold: {sample.gold_sql[:150]}")

    t0 = time.time()
    graph = create_full_graph()
    print(f"  Graph created in {time.time()-t0:.1f}s")

    t0 = time.time()
    try:
        result = graph.invoke(initial_state)
        elapsed = time.time() - t0

        sql = result.get("sql", "")
        exec_result = result.get("exec_result", {})
        token_usage = result.get("token_usage", {})
        trace_events = result.get("trace_events", [])

        print(f"  Elapsed: {elapsed:.1f}s")
        print(f"  Tokens: {token_usage.get('total', 0)} (prompt: {token_usage.get('prompt', 0)}, completion: {token_usage.get('completion', 0)})")
        print(f"  SQL: {sql[:200] if sql else '(empty)'}")
        print(f"  Exec success: {exec_result.get('success', False)}")
        print(f"  Exec error: {(exec_result.get('error') or '')[:150]}")
        print(f"  Trace events: {len(trace_events)}")

        # Show node flow
        nodes_seen = []
        for e in trace_events:
            if e.get("event") == "node_enter":
                nodes_seen.append(e.get("node", "?"))
        if nodes_seen:
            print(f"  Node flow: {' -> '.join(nodes_seen)}")

    except Exception as e:
        elapsed = time.time() - t0
        import traceback
        print(f"  FAILED after {elapsed:.1f}s: {type(e).__name__}: {e}")
        traceback.print_exc()

    sys.stdout.flush()

def main():
    samples = load_bird_dev()
    by_id = {s.question_id: s for s in samples}

    print(f"Database: {Config.DATABASE_URL}")
    print(f"Model: {Config.LLM_CHAT_MODEL}")
    print(f"Timeout: {TIMEOUT_S}s per sample")
    print(f"Targets: {TARGET_IDS}")

    for qid in TARGET_IDS:
        s = by_id.get(qid)
        if s is None:
            print(f"\n#{qid}: NOT FOUND")
            continue
        run_sample(s)

if __name__ == "__main__":
    main()
