"""Router v2 — two-level cascade with LLM borderline gate.

Level 1: Heuristic scoring (zero LLM cost)
  - score 0 → force "simple"
  - score >= 2 → force "complex"
  - score == 1 → borderline → Level 2 LLM gate (binary classify)
"""

import time

import tiktoken

from src.agent.state import AgentState
from nl2sql.config import Config

# GPT-4 / DeepSeek compatible tokenizer
_ENCODER = tiktoken.get_encoding("cl100k_base")

TOKEN_THRESHOLD = 25  # calibrated on 500 BIRD: P90 simple=23, P50 challenging=23
TOKEN_THRESHOLD_HIGH = 35  # very long questions — 0 simple in BIRD, all complex

_MULTI_INTENT_PATTERNS = [
    # Chinese
    "同时", "并且", "分别", "以及",
    "也买了", "还买了",
    "不仅", "还要", "再加上",
    # English — conversational
    "respectively",
    "as well as", "in addition to", "along with",
    "additionally", "furthermore",
    # English — BIRD-style multi-step
    "and also",                     # "show X and also show Y"
    "and then",                     # "find X and then Y"
    "who have both",                # "customers who have both A and B"
    "that have both",               # "products that have both X and Y"
    "and their corresponding",      # "show X and their corresponding Y"
    "along with their",             # "show X along with their Y"
]

# Co-occurrence pairs — both tokens must appear (not adjacent)
_MULTI_INTENT_PAIRS = [
    ("既", "又"),
    ("both", "and"),             # "both X and Y"
    ("not only", "but also"),
    ("first", "then"),           # "first find X, then Y"
]

_COMPLEX_STRUCTURE_PATTERNS = [
    # Chinese
    "占比", "百分比", "增长率",
    "排名", "前5", "前10",
    # English
    "percentage", "ratio", "proportion",
    "growth rate",
    "rank", "ranking",
]

# Implicit complexity signals — checked when score < 2.
# Catch questions that lack explicit structure keywords but are semantically complex.
_IMPLICIT_COMPLEX_PATTERNS = [
    # Chinese — ranking / extremal
    "最", "最高", "最低", "最大", "最小",
    # Chinese — negation
    "没有", "从未", "没买过",
    # Chinese — comparison
    "比", "对比", "比较", "相比",
    # Chinese — per-group
    "每个", "每种", "各类", "各个",
    # English — ranking / extremal
    "highest", "lowest", "maximum", "minimum",
    "most expensive", "least expensive",
    # English — negation (anti-join / NOT EXISTS)
    "never", "no ",
    # English — comparison
    " versus ", "compared to", "difference between",
    " than ",  # "higher than", "more than average", etc.
    # English — per-group
    "per ",
    # ── v2: analysis-driven additions (score=0 gaps) ──
    # Negation variants — anti-join / NOT EXISTS / EXCEPT
    "does not have", "does not contain", "does not include",
    "without ", "excluding ",
    # Self-comparison — "same X as Y", "same X and Y"
    "same ",  # "same eyes, hair and skin colour"
    # Group-wise aggregation — requires window function or GROUP BY + subquery
    "for each ", "respective",
    # Multi-step comparison — "which X has more Y" (implicit comparison)
    "which of the",  # "which of the following" → set comparison
    # Ordinal without rank keyword
    "oldest", "youngest", "most recent", "earliest", "latest",
    # Existence / subquery hints
    "who have ", "that have ", "which have ",
    "two or more ", "three or more ",
    # Compound metrics
    "decrease rate", "completion rate",
]


def _count_entities(question: str) -> int:
    import re
    # Chinese book-name marks
    quoted = re.findall(r'[《「『](.+?)[》」』]', question)
    # English single/double quotes
    quoted += re.findall(r'"([^"]+)"', question)
    quoted += re.findall(r"'([^']+)'", question)
    return len(quoted)


def _heuristic_score(question: str) -> tuple[int, str]:
    """Heuristic scoring. Returns (score, detail).

    score 0: likely simple. 1: borderline. >=2: likely complex.
    detail string describes which signals fired, for logging.
    """
    import re

    token_count = len(_ENCODER.encode(question))
    q = question.lower()  # case-insensitive matching for English patterns
    score = 0
    details: list[str] = []

    # Tiered token threshold: >35 very likely complex (0 simple in BIRD)
    if token_count > TOKEN_THRESHOLD_HIGH:
        score += 2
        details.append(f"tokens>{TOKEN_THRESHOLD_HIGH}")
    elif token_count > TOKEN_THRESHOLD:
        score += 1
        details.append(f"tokens>{TOKEN_THRESHOLD}")

    for pat in _MULTI_INTENT_PATTERNS:
        if pat in q:
            score += 2
            details.append(f"multi_intent:{pat}")
            break

    for a, b in _MULTI_INTENT_PAIRS:
        if a in q and b in q:
            score += 2
            details.append(f"multi_intent_pair:{a}+{b}")
            break

    for pat in _COMPLEX_STRUCTURE_PATTERNS:
        if pat in q:
            score += 1
            details.append(f"structure:{pat}")
            break

    # Parametric structure patterns (regex-based)
    if not details or not any(d.startswith("structure:") for d in details):
        if re.search(r'\btop\s+\d+\b', q):
            score += 1
            details.append("structure:topN")
        elif re.search(r'\bat\s+least\s+\d+\b', q):
            score += 1
            details.append("structure:at_leastN")

    if _count_entities(question) >= 2:
        score += 1
        details.append("entities>=2")

    # Chinese: X品类, X类别, etc.
    entity_hints = re.findall(r'[A-Za-z一-鿿]{2,}(?:品类|类别|分类|商品|产品)', question)
    # English: category/type/class/group keywords
    entity_hints += re.findall(r'\b(?:category|categories|type|types|class|classes|group|groups|brand|product)\b', q)
    if len(entity_hints) >= 2:
        score += 1
        details.append("entity_hints>=2")

    # Multi-step indicators — unconditional, high precision
    # Multiple question marks = two or more distinct questions
    if question.count('?') >= 2:
        score += 1
        details.append("multi_step:multi_q")
    # "total average" / "average total" → contradictory aggregation needs subquery
    if re.search(r'\b(?:total average|average total)\b', q):
        score += 1
        details.append("multi_step:compound_agg")

    # Implicit complexity: fires when score < 2 (not just 0).
    # This prevents strong signals like negation/comparison from being
    # drowned out by weak signals like length.
    if score < 2:
        for pat in _IMPLICIT_COMPLEX_PATTERNS:
            if pat in q:
                score += 1
                details.append(f"implicit:{pat}")
                break

    return score, ",".join(details) if details else "none"


def _llm_classify(question: str) -> tuple[str, str, dict, float]:
    """LLM binary classifier for borderline (score=1) questions.

    Returns (classification, raw_response, token_usage, duration_s) where classification is 'simple' or 'complex'.
    """
    import re
    import logging
    from langchain_core.messages import SystemMessage, HumanMessage
    from src.infrastructure.llm_factory import get_llm

    _log = logging.getLogger(__name__)

    chat = get_llm(temperature=0, max_tokens=10, request_timeout=12, max_retries=0)

    from src.prompts import ROUTER_CLASSIFIER_PROMPT
    system = ROUTER_CLASSIFIER_PROMPT
    msg = HumanMessage(content=f"Question: {question}\n\nClassification:")
    _t0 = time.time()
    response = chat.invoke([SystemMessage(content=system), msg])
    _duration = round(time.time() - _t0, 3)
    raw = response.content.strip()

    tu = response.response_metadata.get("token_usage", {}) if hasattr(response, "response_metadata") else {}
    token_usage = {
        "prompt": tu.get("prompt_tokens", 0),
        "completion": tu.get("completion_tokens", 0),
        "total": tu.get("total_tokens", 0),
    }

    # Extract first word, strip trailing punctuation: "Complex." → "complex"
    first_word = re.split(r"\s+", raw)[0].rstrip(".,;:!?。，；：！？").lower()

    if first_word == "complex":
        return "complex", raw, token_usage, _duration
    elif first_word == "simple":
        return "simple", raw, token_usage, _duration
    else:
        _log.warning("Router LLM returned unexpected response for question %r: %r. Falling back to complex.", question[:80], raw)
        return "complex", raw, token_usage, _duration


def router_node(state: AgentState) -> dict:
    """Classify question. Plan C: heuristic cascade + LLM borderline gate."""
    q = state["question"]

    tlog = state.get("tlog")
    if not tlog:
        from src.obs.logger import TraceLogger
        tlog = TraceLogger()
    tlog.node_enter("router", {"question": q[:60]})

    score, score_detail = _heuristic_score(q)
    token_usage = dict(state.get("token_usage", {"prompt": 0, "completion": 0, "total": 0}))

    if score >= 2:
        complexity = "complex"
        method = "heuristic"
        llm_raw = ""
    elif score == 1:
        llm_dur = 0.0
        try:
            complexity, llm_raw, llm_tu, llm_dur = _llm_classify(q)
            method = "llm_borderline"
        except Exception as e:
            tlog.llm_error(Config.LLM_CHAT_MODEL, type(e).__name__, str(e)[:300])
            # Fallback: treat as complex (safer — enables decomposer + more shots)
            complexity = "complex"
            method = "llm_borderline_error"
            llm_raw = ""
            llm_tu = {}
        tlog.llm_call(Config.LLM_CHAT_MODEL, llm_tu, llm_dur)
        for k in token_usage:
            token_usage[k] += llm_tu.get(k, 0)
    else:
        complexity = "simple"
        method = "heuristic"

    tlog.router_decision(complexity, score, method)
    tlog.node_exit("router", {"complexity": complexity, "score": score, "method": method})

    result: dict = {
        "complexity": complexity,
        "sub_steps": [],
        "router_score": score,
        "router_score_detail": score_detail,
        "router_method": method,
        "tlog": tlog,
        "trace_id": tlog.trace_id,
        "token_usage": token_usage,
    }
    if method == "llm_borderline":
        result["router_llm_raw"] = llm_raw

    return result
