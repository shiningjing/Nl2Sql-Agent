"""Report generation — JSON / CSV / Markdown export for BIRD evaluation results.

Usage:
    from evaluation.reporter import write_report
    write_report(results, cost, output_dir="reports", experiment="ablation")
"""

import csv
import json
import os
from collections import defaultdict
from datetime import datetime


# ── CSV ─────────────────────────────────────────────────────────────────────────

_CSV_COLUMNS = [
    "question_id", "db_id", "difficulty", "knowledge_source",
    "ex", "ves", "detail",
    "gen_sql", "gold_sql",
    "prompt_tokens", "completion_tokens", "total_tokens",
    "elapsed_s", "elapsed_graph_s", "elapsed_execmatch_s",
    "guard_pass", "guard_issue_count", "ast_pass",
    "semantic_pass", "sem_reject_count",
    "retry_count", "candidate_count",
    "decomposer_used", "sub_question_count",
    "rag_recall",
    "trace_id", "last_graph_node",
]


def _flatten_case(r: dict) -> dict:
    """Flatten a case_result dict into a single-level dict for CSV."""
    tu = r.get("token_usage", {}) or {}
    return {
        "question_id": r.get("question_id", ""),
        "db_id": r.get("db_id", ""),
        "difficulty": r.get("difficulty", ""),
        "knowledge_source": r.get("knowledge_source", ""),
        "ex": r.get("ex", False),
        "ves": r.get("ves", 0),
        "detail": r.get("detail", "")[:200],
        "gen_sql": r.get("gen_sql", ""),
        "gold_sql": r.get("gold_sql", ""),
        "prompt_tokens": tu.get("prompt", 0),
        "completion_tokens": tu.get("completion", 0),
        "total_tokens": tu.get("total", 0),
        "elapsed_s": r.get("elapsed_s", 0),
        "elapsed_graph_s": r.get("elapsed_graph_s", 0),
        "elapsed_execmatch_s": r.get("elapsed_execmatch_s", 0),
        "guard_pass": r.get("guard_pass"),
        "guard_issue_count": r.get("guard_issue_count", 0),
        "ast_pass": r.get("ast_pass"),
        "semantic_pass": r.get("semantic_pass"),
        "sem_reject_count": r.get("sem_reject_count", 0),
        "retry_count": r.get("retry_count", 0),
        "candidate_count": r.get("candidate_count", 0),
        "decomposer_used": r.get("decomposer_used", False),
        "sub_question_count": r.get("sub_question_count", 0),
        "rag_recall": r.get("rag_recall"),
        "trace_id": r.get("trace_id", ""),
        "last_graph_node": r.get("last_graph_node", ""),
    }


def write_csv(results: dict, output_dir: str, experiment: str = "") -> str:
    """Write per-sample CSV for all configs. Returns path."""
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    exp_prefix = f"{experiment}_" if experiment else ""
    path = os.path.join(output_dir, f"bird_{exp_prefix}{ts}_cases.csv")

    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for cfg_name, r in results.items():
            for case in r.get("case_results", []):
                row = _flatten_case(case)
                row["_config"] = cfg_name
                writer.writerow(row)

    return path


# ── JSON ────────────────────────────────────────────────────────────────────────

def write_summary_json(results: dict, output_dir: str, experiment: str = "") -> str:
    """Write per-config summary JSON. Returns path."""
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    exp_prefix = f"{experiment}_" if experiment else ""
    path = os.path.join(output_dir, f"bird_{exp_prefix}{ts}_summary.json")

    config_names = list(results.keys())
    summary = {
        "experiment": experiment,
        "timestamp": ts,
        "total_samples": results[config_names[0]]["total"] if config_names else 0,
        "configs": {},
    }

    for name in config_names:
        r = results[name]
        traces = r.get("router_traces", [])
        router_dist = {}
        if traces:
            s_count = sum(1 for t in traces if t.get("router_complexity") == "simple")
            c_count = sum(1 for t in traces if t.get("router_complexity") == "complex")
            s_pass = sum(1 for t in traces if t.get("router_complexity") == "simple" and t.get("ex_pass"))
            c_pass = sum(1 for t in traces if t.get("router_complexity") == "complex" and t.get("ex_pass"))
            router_dist = {
                "simple": s_count, "complex": c_count, "total": len(traces),
                "simple_ex": round(s_pass / s_count, 4) if s_count else 0,
                "complex_ex": round(c_pass / c_count, 4) if c_count else 0,
            }
        summary["configs"][name] = {
            "ex_rate": r["ex_rate"],
            "avg_ves": r["avg_ves"],
            "passed": r["passed"],
            "total": r["total"],
            "crashed": r.get("crashed", 0),
            "avg_tokens": r["avg_tokens"],
            "avg_time_s": r["avg_time_s"],
            "tokens_per_s": r["tokens_per_s"],
            "total_tokens": r["total_tokens"],
            "diff_summary": r["diff_summary"],
            "router_distribution": router_dist,
            "pipeline_stats": r.get("pipeline_stats", {}),
            "avg_rag_recall": r.get("avg_rag_recall"),
            "rag_recall_samples": r.get("rag_recall_samples", 0),
        }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    return path


def write_cost_json(cost: dict, output_dir: str, experiment: str = "") -> str:
    """Write token cost estimate JSON. Returns path."""
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    exp_prefix = f"{experiment}_" if experiment else ""
    path = os.path.join(output_dir, f"bird_{exp_prefix}{ts}_cost.json")

    with open(path, "w", encoding="utf-8") as f:
        json.dump(cost, f, ensure_ascii=False, indent=2)

    return path


# ── Markdown ────────────────────────────────────────────────────────────────────

def _write_pipeline_md(f, results: dict, config_names: list[str]):
    """Pipeline module stats into markdown."""

    # Guard
    f.write("\n## Guard (Hard SQL Validation)\n\n")
    f.write("| Config | Checked | Passed | Rejected | Reject Rate | FN (Pass→EX=0) | FN Rate |\n")
    f.write("|--------|---------|--------|----------|-------------|-----------------|---------|\n")
    for name in config_names:
        g = results[name].get("pipeline_stats", {}).get("guard", {})
        if g:
            f.write(f"| {name} | {g['total_checked']} | {g['passed']} | {g['rejected']} | "
                    f"{g['reject_rate']:.1%} | {g['false_negatives']} | {g['false_neg_rate']:.1%} |\n")

    # SemCheck
    f.write("\n## Semantic Check (LLM Binary YES/NO)\n\n")
    f.write("| Config | Checked | Passed | Rejected | Reject Rate | FN (YES→EX=0) | FN Rate | "
            "FP (NO→EX=1) | Escape Hatch |\n")
    f.write("|--------|---------|--------|----------|-------------|----------------|---------|"
            "--------------|---------------|\n")
    for name in config_names:
        s = results[name].get("pipeline_stats", {}).get("sem_check", {})
        if s:
            f.write(f"| {name} | {s['total_checked']} | {s['passed']} | {s['rejected']} | "
                    f"{s['reject_rate']:.1%} | {s['false_negatives']} | {s['false_neg_rate']:.1%} | "
                    f"{s['false_positives']} | {s['escape_hatch_triggers']} |\n")

    # Self-Correction
    f.write("\n## Self-Correction (Refiner→Generator Loop)\n\n")
    f.write("| Config | Retried | Retry % | Fixed | Fix Rate |\n")
    f.write("|--------|---------|---------|-------|----------|\n")
    for name in config_names:
        c = results[name].get("pipeline_stats", {}).get("self_correction", {})
        if c:
            f.write(f"| {name} | {c['retried']} | {c['retry_pct']:.1%} | "
                    f"{c['retry_fixed']} | {c['fix_rate']:.1%} |\n")

    # Voter
    has_multi = any(
        results[n].get("pipeline_stats", {}).get("voter", {}).get("multi_enabled_samples", 0) > 0
        for n in config_names
    )
    if has_multi:
        f.write("\n## Voter (Multi-Candidate Execution Voting)\n\n")
        f.write("| Config | Multi Samples | Avg Candidates | Dist 1 | Dist 2 | Dist 3 |\n")
        f.write("|--------|---------------|----------------|--------|--------|--------|\n")
        for name in config_names:
            v = results[name].get("pipeline_stats", {}).get("voter", {})
            if v and v.get("multi_enabled_samples", 0) > 0:
                cd = v.get("candidate_distribution", {})
                f.write(f"| {name} | {v['multi_enabled_samples']} | {v['avg_candidates']} | "
                        f"{cd.get('1', 0)} | {cd.get('2', 0)} | {cd.get('3', 0)} |\n")

    # Decomposer
    has_dec = any(
        results[n].get("pipeline_stats", {}).get("decomposer", {}).get("used", 0) > 0
        for n in config_names
    )
    if has_dec:
        f.write("\n## Decomposer (Complex Question Decomposition)\n\n")
        f.write("| Config | Samples | EX | EX Rate | Avg Sub-Questions |\n")
        f.write("|--------|---------|-----|---------|-------------------|\n")
        for name in config_names:
            d = results[name].get("pipeline_stats", {}).get("decomposer", {})
            if d and d.get("used", 0) > 0:
                f.write(f"| {name} | {d['used']} | {d['ex']} | {d['ex_rate']:.1%} | "
                        f"{d['avg_sub_questions']} |\n")

    # Node Timing
    f.write("\n## Node Timing (Avg per Sample)\n\n")
    all_nodes: set[str] = set()
    for name in config_names:
        nt = results[name].get("pipeline_stats", {}).get("node_timing_avg", {})
        all_nodes.update(nt.keys())
    if all_nodes:
        sorted_nodes = sorted(all_nodes)
        f.write("| Config | " + " | ".join(sorted_nodes) + " |\n")
        f.write("|--------|" + "|".join(["--------"] * len(sorted_nodes)) + "|\n")
        for name in config_names:
            nt = results[name].get("pipeline_stats", {}).get("node_timing_avg", {})
            vals = [f"{nt.get(n, 0):.1f}s" for n in sorted_nodes]
            f.write(f"| {name} | " + " | ".join(vals) + " |\n")

    # RAG Table Recall
    has_rag = any(results[n].get("avg_rag_recall") is not None for n in config_names)
    if has_rag:
        f.write("\n## RAG Table Recall\n\n")
        f.write("| Config | Samples | Avg Recall |\n")
        f.write("|--------|---------|------------|\n")
        for name in config_names:
            rr = results[name].get("avg_rag_recall")
            if rr is not None:
                f.write(f"| {name} | {results[name].get('rag_recall_samples', 0)} | {rr:.1%} |\n")


def write_summary_md(results: dict, cost: dict, output_dir: str, experiment: str = "") -> str:
    """Write evaluation summary as Markdown. Returns path."""
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    exp_prefix = f"{experiment}_" if experiment else ""
    path = os.path.join(output_dir, f"bird_{exp_prefix}{ts}_summary.md")

    config_names = list(results.keys())
    n_total = results[config_names[0]]["total"] if config_names else 0

    with open(path, "w", encoding="utf-8") as f:
        f.write(f"# BIRD Mini-Dev Evaluation — {experiment}\n\n")
        f.write(f"**Date**: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"**Samples**: {n_total}\n\n")

        # Overall
        f.write("## Overall\n\n")
        f.write("| Config | EX | VES | Passed | Crashed | Avg Time | Avg Tokens | Tok/s | RAG Recall |\n")
        f.write("|--------|-----|-----|--------|---------|----------|------------|-------|------------|\n")
        for name in config_names:
            r = results[name]
            cr = r.get("crashed", 0)
            cr_str = f"⚠{cr}" if cr else "0"
            rr = r.get("avg_rag_recall")
            rr_str = f"{rr:.1%} ({r.get('rag_recall_samples', 0)})" if rr is not None else "-"
            f.write(f"| {name} | {r['ex_rate']:.1%} | {r['avg_ves']:.4f} | "
                    f"{r['passed']}/{r['total']} | {cr_str} | {r['avg_time_s']}s | "
                    f"{r['avg_tokens']} | {r['tokens_per_s']:.0f} | {rr_str} |\n")

        # EX by Difficulty
        f.write("\n## EX by Difficulty\n\n")
        f.write("| Config | Simple | Moderate | Challenging |\n")
        f.write("|--------|--------|----------|-------------|\n")
        for name in config_names:
            r = results[name]
            ds = r.get("diff_summary", {})
            parts = []
            for d in ["simple", "moderate", "challenging"]:
                if d in ds:
                    dd = ds[d]
                    parts.append(f"{dd['ex']:.1%} ({dd['passed']}/{dd['total']})")
                else:
                    parts.append("-")
            f.write(f"| {name} | {' | '.join(parts)} |\n")

        # Router Distribution
        has_router = any(results[n].get("router_traces") for n in config_names)
        if has_router:
            f.write("\n## Router Distribution\n\n")
            f.write("| Config | Router Simple | Router Complex | Simple EX | Complex EX |\n")
            f.write("|--------|---------------|----------------|-----------|------------|\n")
            for name in config_names:
                traces = results[name].get("router_traces", [])
                if traces:
                    s = sum(1 for t in traces if t.get("router_complexity") == "simple")
                    c = sum(1 for t in traces if t.get("router_complexity") == "complex")
                    s_pass = sum(1 for t in traces if t.get("router_complexity") == "simple" and t.get("ex_pass"))
                    c_pass = sum(1 for t in traces if t.get("router_complexity") == "complex" and t.get("ex_pass"))
                    s_ex = f"{s_pass/s:.1%}" if s else "-"
                    c_ex = f"{c_pass/c:.1%}" if c else "-"
                    f.write(f"| {name} | {s} | {c} | {s_ex} | {c_ex} |\n")

        # Pipeline module stats
        has_pipeline = any(results[n].get("pipeline_stats") for n in config_names)
        if has_pipeline:
            _write_pipeline_md(f, results, config_names)

        # Cost
        _write_cost_md(f, cost)

    return path


def _write_cost_md(f, cost: dict):
    """Token cost section in markdown."""
    if "test_run" in cost:
        tr = cost["test_run"]
        fp = cost["full_run_prediction"]
        f.write(f"\n## Token Cost\n\n")
        f.write(f"**Test run**: {tr['total_calls']} calls, {tr['total_tokens']:,} tokens, "
                f"${tr['cost_total_usd']:.4f}\n\n")
        f.write(f"**Full run prediction** ({fp['total_calls']} calls): "
                f"~{fp['estimated_tokens']:,} tokens, "
                f"best ${fp['estimated_cost_usd_best']:.2f}, "
                f"with retries ${fp['estimated_cost_usd_with_retries']:.2f}\n")
    elif "per_source" in cost:
        f.write(f"\n## Token Cost\n\n")
        for ks, c in cost["per_source"].items():
            if isinstance(c, dict):
                tr = c.get("test_run", {})
                fp = c.get("full_run_prediction", {})
                if tr:
                    f.write(f"### {ks}\n")
                    f.write(f"- Test: {tr.get('total_calls')} calls, {tr.get('total_tokens',0):,} tokens, "
                            f"${tr.get('cost_total_usd',0):.4f}\n")
                if fp:
                    f.write(f"- Predicted: {fp.get('total_calls')} calls, "
                            f"~{fp.get('estimated_tokens',0):,} tokens, "
                            f"best ${fp.get('estimated_cost_usd_best',0):.2f}\n")
            f.write("\n")


# ── Master ──────────────────────────────────────────────────────────────────────

def write_report(results: dict, cost: dict, output_dir: str, experiment: str = "") -> dict[str, str]:
    """Generate all reports (JSON, CSV, MD) and return path map.

    Returns {"json": ..., "cost": ..., "csv": ..., "md": ...}
    """
    paths = {}

    json_path = write_summary_json(results, output_dir, experiment)
    paths["json"] = json_path

    cost_path = write_cost_json(cost, output_dir, experiment)
    paths["cost"] = cost_path

    csv_path = write_csv(results, output_dir, experiment)
    paths["csv"] = csv_path

    md_path = write_summary_md(results, cost, output_dir, experiment)
    paths["md"] = md_path

    return paths
