"""Debug script: run selected questions through Full Graph and show per-node details."""
import json, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.agent.graphs.full_graph import create_full_graph
from src.agent.nodes.router import _heuristic_score
from src.eval.bird_loader import load_bird_dev, get_database_url

# ── Select test questions ──
TEST_CASES = [
    # (question_id as string, label)
    ("1198", "Score=0 | simple  | PASS — baseline: simple path works"),
    ("850",  "Score=0 | simple  | PASS — simple path, different DB"),
    ("1483", "Score=0 | simple  | FAIL — why does simple path fail on BIRD simple?"),
    ("11",   "Score=0 | simple  | FAIL — simple path, california_schools"),
    ("1471", "Score=1 | simple  | FAIL — LLM borderline, ratio question"),
    ("1484", "Score=2 | simple  | FAIL — force complex, still fails"),
]

samples = load_bird_dev()
sample_map = {s.question_id: s for s in samples}
print(f"Loaded {len(samples)} samples, first ID: {samples[0].question_id!r}")

for qid_str, label in TEST_CASES:
    s = sample_map.get(qid_str)
    if s is None:
        print(f"\n❌ Question #{qid_str} NOT FOUND in dataset!")
        continue

    score, detail = _heuristic_score(s.question)
    db_url = get_database_url(s)

    print(f"\n{'='*80}")
    print(f"#{qid_str}  [{s.db_id}]  [{s.difficulty}]  {label}")
    print(f"Q: {s.question}")
    print(f"Heuristic: score={score}  detail={detail}")
    print(f"Gold SQL: {s.gold_sql[:200]}")
    print(f"{'='*80}")

    t0 = time.time()
    try:
        graph = create_full_graph()
        state = graph.invoke({
            "question": s.question,
            "db_id": s.db_id,
            "rag_schema": True,
            "rag_domain": True,
            "skip_schema": False,
            "sample_rows": True,
            "multi_candidate": True,
            "rag_k": 8,
            "rag_column_prune": True,
            "fewshot_enabled": True,
            "database_url": db_url,
        })
    except Exception as e:
        print(f"  ❌ graph.invoke() exception: {type(e).__name__}: {str(e)[:300]}")
        continue
    elapsed = time.time() - t0

    # ── Node-by-node summary ──
    router_score = state.get("router_score", "?")
    router_method = state.get("router_method", "?")
    complexity = state.get("complexity", "?")
    print(f"\n  [Router]       score={router_score} method={router_method} → {complexity}")

    sub_steps = state.get("sub_steps", [])
    if sub_steps:
        print(f"  [Decomposer]   {len(sub_steps)} sub-questions:")
        for i, step in enumerate(sub_steps):
            print(f"    {i+1}. {step.get('question', str(step)[:150])}")
    else:
        print(f"  [Decomposer]   skipped (simple path)")

    fewshot_hits = state.get("fewshot_hits", 0)
    print(f"  [Fewshot]      {fewshot_hits} examples retrieved")

    gen_sql = state.get("sql", "")
    candidate_sqls = state.get("candidate_sqls", [])
    print(f"  [Generator]    {len(candidate_sqls)} candidates generated")
    for i, csql in enumerate(candidate_sqls[:3]):
        print(f"    candidate {i}: {csql[:180]}")

    guard_pass = state.get("guard_pass", False)
    guard_issues = state.get("guard_issues", [])
    print(f"  [Guard]        {'PASS' if guard_pass else 'FAIL'} ({len(guard_issues)} issues)")
    for issue in guard_issues[:5]:
        print(f"    - [{issue.get('type', '?')}] {issue.get('detail', str(issue)[:150])}")

    exec_result = state.get("exec_result", {})
    winner_idx = state.get("winner_idx", -1)
    print(f"  [Voter]        winner=candidate_{winner_idx}  exec_success={exec_result.get('success')}")
    if not exec_result.get('success'):
        print(f"    error: {exec_result.get('error', 'N/A')[:200]}")
    else:
        print(f"    rows={exec_result.get('row_count')} cols={exec_result.get('columns')}")

    semantic_pass = state.get("semantic_pass", True)
    semantic_reason = state.get("semantic_reason", "")
    print(f"  [SemanticCheck] {'PASS' if semantic_pass else 'FAIL'}")
    if semantic_reason:
        print(f"    reason: {semantic_reason[:200]}")

    retry_count = state.get("retry_count", 0)
    print(f"  [Refiner]      retries used: {retry_count}")

    # ── Final SQL ──
    print(f"\n  ── Final SQL ({len(gen_sql)} chars) ──")
    print(f"  {gen_sql[:600]}")

    # ── Gold comparison ──
    print(f"\n  ── Gold SQL ──")
    print(f"  {s.gold_sql[:400]}")

    # ── Token usage ──
    tu = state.get("token_usage", {})
    print(f"\n  Tokens: {tu.get('total', 0):,}  |  Elapsed: {elapsed:.1f}s")
    tlog = state.get("tlog")
    if tlog:
        timings = tlog.get_node_timings()
        if timings:
            print(f"  Node timings: ", end="")
            for node, dur in sorted(timings.items(), key=lambda x: -x[1]):
                print(f"{node}={dur:.1f}s ", end="")
            print()
