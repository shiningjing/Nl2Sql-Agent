"""诊断脚本：测量 graph.invoke vs exec_match 各阶段耗时。

从 3 个 DB 大小量级各选代表 DB，每 DB 取 3 题，R0_Baseline 配置。
"""
import os, sys, json, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collections import defaultdict
from src.agent.graphs.full_graph import create_full_graph
from src.eval.bird_loader import load_bird_dev, get_database_url
from nl2sql.execute import execute_sql

# ── 选样：小/中/大 各 2 DB，每 DB 3 题 ──
DB_TIERS = {
    "small":  ["thrombosis_prediction", "california_schools"],
    "medium": ["financial", "formula_1"],
    "large":  ["card_games", "codebase_community"],
}
QUESTIONS_PER_DB = 3

def main():
    all_samples = load_bird_dev()
    samples_by_db = defaultdict(list)
    for s in all_samples:
        samples_by_db[s.db_id].append(s)

    graph = create_full_graph()
    results = []

    for tier, db_ids in DB_TIERS.items():
        for db_id in db_ids:
            db_samples = samples_by_db.get(db_id, [])
            if not db_samples:
                print(f"  [WARN] DB {db_id} not found, skip")
                continue
            picked = db_samples[:QUESTIONS_PER_DB]
            db_url = get_database_url(picked[0])
            db_path = picked[0].database_path
            db_size_mb = round(os.path.getsize(db_path) / (1024 * 1024), 1) if os.path.exists(db_path) else 0

            for s in picked:
                print(f"  [{tier}] {db_id} ({db_size_mb}MB)  Q{s.question_id}: {s.question[:60]}...")

                # ── Phase 1: graph.invoke ──
                t0 = time.time()
                state = graph.invoke({
                    "question": s.question,
                    "db_id": db_id,
                    "rag_schema": True,
                    "rag_domain": True,
                    "multi_candidate": True,
                    "rag_k": 8,
                    "rag_column_prune": False,
                    "fewshot_enabled": False,
                    "database_url": db_url,
                })
                graph_time = time.time() - t0

                gen_sql = state.get("sql", "")
                exec_result = state.get("exec_result", {})

                # ── Phase 2: 模拟 exec_match → 执行 gold SQL ──
                t0 = time.time()
                r_gold = execute_sql(s.gold_sql, database_url=db_url)
                gold_time = time.time() - t0

                # ── Phase 3: 模拟 exec_match → 重复执行 gen SQL ──
                t0 = time.time()
                r_gen = execute_sql(gen_sql, database_url=db_url) if gen_sql else {"success": False, "error": "no sql"}
                gen_repeat_time = time.time() - t0

                # ── 汇总 ──
                total = graph_time + gold_time + gen_repeat_time
                results.append({
                    "tier": tier,
                    "db": db_id,
                    "db_mb": db_size_mb,
                    "qid": s.question_id,
                    "graph_s": round(graph_time, 2),
                    "gold_s": round(gold_time, 2),
                    "gen_repeat_s": round(gen_repeat_time, 2),
                    "gen_ok": r_gen["success"],
                    "gold_ok": r_gold["success"],
                    "exec_in_graph": exec_result.get("success", False) if exec_result else False,
                })

                print(f"         graph={graph_time:.1f}s  gold_exec={gold_time:.1f}s  "
                      f"gen_repeat={gen_repeat_time:.1f}s  total={total:.1f}s")

    # ── 汇总表格 ──
    print("\n" + "=" * 90)
    print("PER-TIER SUMMARY")
    print("=" * 90)
    for tier in ["small", "medium", "large"]:
        tier_results = [r for r in results if r["tier"] == tier]
        if not tier_results:
            continue
        n = len(tier_results)
        avg_graph = sum(r["graph_s"] for r in tier_results) / n
        avg_gold = sum(r["gold_s"] for r in tier_results) / n
        avg_repeat = sum(r["gen_repeat_s"] for r in tier_results) / n
        avg_total = sum(r["graph_s"] + r["gold_s"] + r["gen_repeat_s"] for r in tier_results) / n
        repeat_pct = (avg_repeat / avg_total * 100) if avg_total > 0 else 0
        print(f"  {tier:>6s} ({n} samples): "
              f"graph={avg_graph:.1f}s  gold={avg_gold:.1f}s  "
              f"gen_repeat={avg_repeat:.1f}s  total={avg_total:.1f}s  "
              f"repeat%={repeat_pct:.0f}%")

    print("\n  NOTE: gen_repeat = 多余的 gen_sql 重复执行 (exec_match 内)")
    print("        gold      = exec_match 内的 gold SQL 执行 (必要)")
    print("        graph     = graph.invoke() 总时间 (含 LLM + 内部 executor)")

    # ── 写 JSON ──
    out_path = os.path.join(os.path.dirname(__file__), "..", "reports", "timing_diagnosis.json")
    with open(os.path.abspath(out_path), "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n  Raw data → {os.path.abspath(out_path)}")


if __name__ == "__main__":
    main()
