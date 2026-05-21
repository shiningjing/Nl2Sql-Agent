"""Debug script: run challenging BIRD questions with full traceback capture."""
import json
import os
import sys
import time
import traceback
import logging

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)
os.chdir(_PROJECT_ROOT)  # CRITICAL: ChromaDB uses relative path ./.chroma

from src.eval.bird_loader import load_bird_dev, get_database_url
from src.agent.graphs.full_graph import create_full_graph

# Only log warnings and above (reduce noise)
logging.basicConfig(level=logging.WARNING, format='%(asctime)s [%(name)s] %(levelname)s: %(message)s')

# Load data
samples = load_bird_dev()
challenging = [s for s in samples if s.difficulty == "challenging"]
print(f"Found {len(challenging)} challenging samples")

# Pick first 5 challenging samples
test_samples = challenging[:5]
print(f"Testing {len(test_samples)} samples: {[(s.question_id, s.db_id) for s in test_samples]}")

config = {
    "rag_schema": True,
    "rag_domain": True,
    "multi_candidate": True,
    "rag_column_prune": True,
    "fewshot_enabled": True,
    "sample_rows": True,
    "k": 8,
}

for i, sample in enumerate(test_samples):
    print(f"\n{'='*80}")
    print(f"[{i+1}/{len(test_samples)}] Q{sample.question_id} | {sample.db_id} | {sample.difficulty}")
    print(f"Q: {sample.question[:200]}")
    print(f"{'='*80}")

    database_url = get_database_url(sample)
    t0 = time.time()

    try:
        graph = create_full_graph()
        print(f"[DEBUG] Graph created, invoking...")

        state_input = {
            "question": sample.question,
            "db_id": sample.db_id,
            "rag_schema": config["rag_schema"],
            "rag_domain": config["rag_domain"],
            "skip_schema": config.get("skip_schema", False),
            "sample_rows": config.get("sample_rows", True),
            "multi_candidate": config.get("multi_candidate", False),
            "rag_k": config.get("k", 8),
            "rag_column_prune": config.get("rag_column_prune", False),
            "fewshot_enabled": config.get("fewshot_enabled", False),
            "database_url": database_url,
            "_domain_notes_override": "",
        }
        print(f"[DEBUG] state_input keys: {list(state_input.keys())}")

        state = graph.invoke(state_input)

        elapsed = time.time() - t0
        gen_sql = state.get("sql", "")
        tlog = state.get("tlog")

        print(f"[OK] Completed in {elapsed:.1f}s")
        print(f"  SQL: {gen_sql[:200] if gen_sql else '(empty)'}")
        print(f"  tlog present: {tlog is not None}")
        if tlog:
            print(f"  trace events: {len(tlog.events)}")
            for evt in tlog.events[:10]:
                print(f"    - {evt.get('event')} @ {evt.get('node', '?')}")
        else:
            print(f"  NO tlog!")

        print(f"  State keys: {list(state.keys())}")
        print(f"  complexity: {state.get('complexity')}")
        print(f"  router_method: {state.get('router_method')}")
        print(f"  router_score: {state.get('router_score')}")

    except Exception as e:
        elapsed = time.time() - t0
        print(f"[FAIL] Exception after {elapsed:.1f}s: {type(e).__name__}: {e}")
        print(f"[FAIL] Full traceback:")
        traceback.print_exc()
