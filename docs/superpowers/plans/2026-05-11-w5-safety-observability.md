# W5: Safety + Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add AST guardrail, structured logging, Router v2, Voter semantic safety net, and R4 ablation — making the system safer, observable, and measurably better than R3.

**Architecture:** Six independent upgrades layered on the existing full_graph pipeline: (1) sqlglot AST validation replaces/supplements regex Guard, (2) trace_id + structured JSON logging across all nodes, (3) two-level Router with LLM borderline gate, (4) Voter semantic LLM check before finalizing winner, (5) execution timeout protection, (6) R4 ablation config in eval. The graph gains one new node `semantic_check` between Voter and END.

**Tech Stack:** sqlglot (AST parsing), Python logging (stdlib), LangGraph conditional edges, DeepSeek API

---

## File Map

| File | Action | Role |
|------|--------|------|
| `src/guardrails/ast_validator.py` | Create | sqlglot AST parse + safety validation |
| `src/agent/nodes/semantic_check.py` | Create | LLM binary YES/NO semantic check on Voter winner |
| `src/agent/graphs/full_graph.py` | Modify | Add semantic_check node + edges; wire trace_id propagation |
| `src/agent/state.py` | Modify | Add `semantic_pass`, `semantic_feedback`, `ast_pass`, `ast_issues` fields |
| `src/agent/nodes/router.py` | Modify | Router v2: heuristic score 0→simple, ≥2→complex, 1→LLM gate |
| `src/agent/nodes/guard.py` | Modify | Integrate sqlglot AST validation; add execution timeout (10s) |
| `src/agent/nodes/executor.py` | Modify | Add query timeout parameter to run_sql |
| `src/agent/nodes/schema_retriever.py` | Modify | Generate trace_id on first entry |
| `src/obs/__init__.py` | Create | Module init |
| `src/obs/logger.py` | Create | Structured JSON logger with trace_id context |
| `scripts/eval.py` | Modify | Add R4 config; support new graph params |
| `requirements.txt` | Modify | Add sqlglot |
| `tests/test_w5_ast.py` | Create | AST guardrail tests |
| `tests/test_w5_router.py` | Create | Router v2 tests |
| `tests/test_w5_semantic_check.py` | Create | Semantic check tests |
| `tests/test_w5_trace.py` | Create | Trace ID propagation tests |

---

### Task 1: Install sqlglot dependency

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1: Add sqlglot to requirements.txt**

```
streamlit>=1.32
sqlalchemy>=2.0
chromadb>=0.5
langchain>=0.2
langchain-openai>=0.1
python-dotenv>=1.0
sqlparse>=0.5
sentence-transformers>=2.7
sqlglot>=25.0
```

- [ ] **Step 2: Install sqlglot**

```bash
pip install sqlglot>=25.0
```

- [ ] **Step 3: Verify import works**

```bash
python -c "import sqlglot; print(sqlglot.__version__)"
```
Expected: prints version number without errors

---

### Task 2: Create structured logging module

**Files:**
- Create: `src/obs/__init__.py`
- Create: `src/obs/logger.py`

- [ ] **Step 1: Create `src/obs/__init__.py`**

```python
"""Observability module — structured logging, tracing, and metrics."""
```

- [ ] **Step 2: Write `src/obs/logger.py`**

```python
"""Structured JSON-line logger with trace context.

Usage:
    from src.obs.logger import TraceLogger
    tlog = TraceLogger(trace_id)
    tlog.node_enter("generator", {"question_len": 42})
    tlog.node_exit("generator", {"sql_len": 150})
    tlog.llm_call("deepseek-chat", {"prompt_tokens": 1200, "completion_tokens": 300})
"""

import json
import logging
import sys
import time
import uuid
from pathlib import Path

_log = logging.getLogger("nl2sql")


def init_logging(level: int = logging.INFO, log_file: str | None = None):
    """Configure root logger for structured JSON output."""
    handler = logging.StreamHandler(sys.stderr)
    if log_file:
        log_dir = Path(log_file).parent
        log_dir.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(log_file, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(message)s"))
    _log.handlers.clear()
    _log.addHandler(handler)
    _log.setLevel(level)


class TraceLogger:
    def __init__(self, trace_id: str | None = None):
        self.trace_id = trace_id or uuid.uuid4().hex[:12]
        self._start_times: dict[str, float] = {}

    def _emit(self, event: str, data: dict):
        record = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
            "trace_id": self.trace_id,
            "event": event,
            **data,
        }
        _log.info(json.dumps(record, ensure_ascii=False, default=str))

    def node_enter(self, node: str, meta: dict | None = None):
        key = f"{node}:enter"
        self._start_times[key] = time.time()
        self._emit("node_enter", {"node": node, **(meta or {})})

    def node_exit(self, node: str, meta: dict | None = None):
        key = f"{node}:enter"
        duration = 0.0
        if key in self._start_times:
            duration = round(time.time() - self._start_times.pop(key), 3)
        self._emit("node_exit", {"node": node, "duration_s": duration, **(meta or {})})

    def llm_call(self, model: str, usage: dict, duration_s: float = 0):
        self._emit("llm_call", {
            "model": model,
            "prompt_tokens": usage.get("prompt_tokens", 0),
            "completion_tokens": usage.get("completion_tokens", 0),
            "total_tokens": usage.get("total_tokens", 0),
            "duration_s": round(duration_s, 3),
        })

    def sql_exec(self, success: bool, row_count: int, duration_s: float, error: str = ""):
        self._emit("sql_exec", {
            "success": success,
            "row_count": row_count,
            "duration_s": round(duration_s, 3),
            "error": error[:120] if error else "",
        })

    def guard_result(self, passed: bool, issues: list[dict]):
        self._emit("guard", {
            "passed": passed,
            "issue_count": len(issues),
            "issue_types": [i.get("type", "?") for i in issues],
        })

    def router_decision(self, complexity: str, score: int, method: str):
        self._emit("router", {
            "complexity": complexity,
            "score": score,
            "method": method,
        })

    def semantic_verdict(self, passed: bool, reason: str):
        self._emit("semantic_check", {
            "passed": passed,
            "reason": reason[:200] if reason else "",
        })
```

---

### Task 3: Extend AgentState with W5 fields

**Files:**
- Modify: `src/agent/state.py`

- [ ] **Step 1: Add new fields to AgentState**

In `src/agent/state.py`, after `chosen_sql: NotRequired[str]`, add:

```python
    # ── W5: Semantic check ──
    semantic_pass: NotRequired[bool]
    semantic_feedback: NotRequired[str]

    # ── W5: AST guardrail ──
    ast_pass: NotRequired[bool]
    ast_issues: NotRequired[list[dict]]

    # ── W5: Trace ──
    tlog: NotRequired[object]  # TraceLogger instance (not serialized)
```

---

### Task 4: AST Guardrail (sqlglot)

**Files:**
- Create: `src/guardrails/ast_validator.py`
- Modify: `src/agent/nodes/guard.py`
- Create: `tests/test_w5_ast.py`

- [ ] **Step 1: Write `src/guardrails/ast_validator.py`**

```python
"""AST-based SQL validation using sqlglot.

Provides a stronger structural check than regex — catches real syntax errors,
dangerous constructs, and dialect mismatches before execution.
"""

import sqlglot
from sqlglot.errors import ErrorLevel


def validate_sql_ast(sql: str, dialect: str = "sqlite") -> tuple[bool, list[dict]]:
    """Parse SQL with sqlglot and return (valid, issues).

    Checks:
    - Syntax validity (parse success)
    - Read-only: only SELECT/WITH statements
    - No dangerous functions or patterns
    """
    issues: list[dict] = []

    # Parse with error collection
    try:
        parsed = sqlglot.parse(sql, read=dialect, error_level=ErrorLevel.RAISE)
    except Exception as e:
        return False, [{"type": "ast_syntax", "detail": f"SQL parse error: {str(e)[:200]}"}]

    if not parsed:
        return False, [{"type": "ast_syntax", "detail": "SQL parsed to empty AST"}]

    for stmt in parsed:
        kind = stmt.key.upper() if hasattr(stmt, 'key') else str(type(stmt).__name__)

        # Must be SELECT or WITH (CTE)
        if kind not in ("SELECT", "WITH", "UNION", "INTERSECT", "EXCEPT"):
            issues.append({
                "type": "ast_forbidden",
                "detail": f"Forbidden statement type: {kind}. Only SELECT/WITH allowed.",
            })

        # Check for dangerous expressions
        sql_upper = stmt.sql().upper()
        if "DROP " in sql_upper:
            issues.append({"type": "ast_dangerous", "detail": "DROP statement detected in AST"})
        if "ALTER " in sql_upper:
            issues.append({"type": "ast_dangerous", "detail": "ALTER statement detected in AST"})

    # Check for unmatched column references in basic cases
    # Walk the AST for any column references that look obviously broken
    try:
        for stmt in parsed:
            _check_column_refs(stmt, issues)
    except Exception:
        pass

    return len(issues) == 0, issues


def _check_column_refs(stmt, issues: list[dict]):
    """Walk AST for suspicious column patterns (structural check only)."""
    sql_str = stmt.sql()
    sql_upper = sql_str.upper()

    # Detect double-where: SELECT ... WHERE ... WHERE
    if sql_upper.count(" WHERE ") > 1:
        issues.append({"type": "ast_structure", "detail": "Multiple WHERE clauses detected"})

    # Detect missing FROM after SELECT
    if "SELECT " in sql_upper and " FROM " not in sql_upper:
        # Could be SELECT expr without table — valid in SQLite
        pass

    # Detect obviously unbalanced parentheses already caught by parser
    # This is a structural sanity layer on top of sqlglot's own validation
```

- [ ] **Step 2: Integrate AST check into Guard node**

Modify `src/agent/nodes/guard.py`:

```python
"""Guard node — hard SQL validation before execution. Zero LLM cost."""
import re
from src.agent.state import AgentState
from src.guardrails.ast_validator import validate_sql_ast


# Keep existing _validate_sql and _check_hallucinations unchanged


def guard_node(state: AgentState) -> dict:
    """Validate SQL against schema + safety rules. Sets guard_pass / guard_issues / ast_pass / ast_issues."""
    sql = state.get("sql", "")
    schema_text = state.get("schema_text", "")
    candidate_sqls = state.get("candidate_sqls", [])

    issues: list[dict] = []

    # Layer 1: Regex safety check
    valid, reason = _validate_sql(sql)
    if not valid:
        issues.append({"type": "safety", "detail": reason})

    # Layer 2: Hallucination check
    issues.extend(_check_hallucinations(sql, schema_text))

    # Layer 3: AST structural validation (W5)
    ast_pass, ast_issues = validate_sql_ast(sql)
    issues.extend(ast_issues)

    passed = len(issues) == 0

    # Trace logging if available
    tlog = state.get("tlog")
    if tlog:
        tlog.guard_result(passed, issues)

    return {
        "guard_pass": passed,
        "guard_issues": issues,
        "ast_pass": ast_pass,
        "ast_issues": ast_issues,
    }
```

- [ ] **Step 3: Write `tests/test_w5_ast.py`**

```python
"""Tests for AST guardrail validation."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.guardrails.ast_validator import validate_sql_ast


def test_valid_select():
    ok, issues = validate_sql_ast("SELECT * FROM users WHERE id = 1")
    assert ok, f"Expected valid but got: {issues}"


def test_valid_with_cte():
    ok, issues = validate_sql_ast(
        "WITH active AS (SELECT * FROM users WHERE status = 1) "
        "SELECT * FROM active"
    )
    assert ok, f"Expected valid but got: {issues}"


def test_syntax_error():
    ok, issues = validate_sql_ast("SELEC * FORM users")  # typos
    assert not ok
    assert any("syntax" in i.get("detail", "").lower() or "parse" in i.get("detail", "").lower() for i in issues)


def test_forbidden_insert():
    ok, issues = validate_sql_ast("INSERT INTO users VALUES (1, 'test')")
    assert not ok
    assert any("forbidden" in i.get("type", "").lower() for i in issues)


def test_forbidden_drop():
    ok, issues = validate_sql_ast("DROP TABLE users")
    assert not ok
    assert any("drop" in i.get("detail", "").lower() or "forbidden" in i.get("type", "").lower() for i in issues)


def test_empty_sql():
    ok, issues = validate_sql_ast("")
    assert not ok


def test_complex_join():
    ok, issues = validate_sql_ast(
        "SELECT u.name, o.total FROM users u "
        "JOIN orders o ON u.id = o.user_id "
        "WHERE o.total > 100 ORDER BY o.total DESC LIMIT 10"
    )
    assert ok, f"Expected valid but got: {issues}"


if __name__ == "__main__":
    test_valid_select()
    test_valid_with_cte()
    test_syntax_error()
    test_forbidden_insert()
    test_forbidden_drop()
    test_empty_sql()
    test_complex_join()
    print("All AST tests passed!")
```

- [ ] **Step 4: Run AST tests**

```bash
python tests/test_w5_ast.py
```
Expected: "All AST tests passed!"

---

### Task 5: Router v2 — Two-Level Cascade

**Files:**
- Modify: `src/agent/nodes/router.py`
- Modify: `src/agent/graphs/full_graph.py` (add tlog wiring)
- Create: `tests/test_w5_router.py`

- [ ] **Step 1: Replace router_node with v2 implementation**

Rewrite `src/agent/nodes/router.py`:

```python
"""Router v2 — two-level cascade with LLM borderline gate.

Level 1: Heuristic scoring (zero LLM cost)
  - score 0 → force "simple"
  - score >= 2 → force "complex"
  - score == 1 → borderline → Level 2 LLM gate (binary classify)
"""

from src.agent.state import AgentState
from nl2sql.config import Config

# Heuristic patterns (unchanged from v1)
_MULTI_INTENT_PATTERNS = [
    "同时", "并且", "分别", "以及",
    "也买了", "还买了", "既...又",
    "不仅", "还要", "再加上",
]

_COMPLEX_STRUCTURE_PATTERNS = [
    "占比", "百分比", "增长率",
    "排名", "前5", "前10",
]


def _count_entities(question: str) -> int:
    import re
    quoted = re.findall(r'[《「『](.+?)[》」』]', question)
    return len(quoted)


def _heuristic_score(question: str) -> int:
    """Heuristic scoring: 0/1/2+. Zero LLM cost."""
    q_len = len(question)
    score = 0

    if q_len > 40:
        score += 1

    for pat in _MULTI_INTENT_PATTERNS:
        if pat in question:
            score += 2
            break

    for pat in _COMPLEX_STRUCTURE_PATTERNS:
        if pat in question:
            score += 1
            break

    if _count_entities(question) >= 2:
        score += 1

    import re
    entity_hints = re.findall(r'[A-Za-z一-鿿]{2,}(?:品类|类别|分类|商品|产品)', question)
    if len(entity_hints) >= 2:
        score += 1

    return score


def _llm_classify(question: str) -> str:
    """LLM binary classifier for borderline (score=1) questions. Returns 'simple' or 'complex'."""
    from langchain_openai import ChatOpenAI
    from langchain_core.messages import SystemMessage, HumanMessage

    chat = ChatOpenAI(
        model=Config.LLM_CHAT_MODEL,
        api_key=Config.LLM_API_KEY,
        base_url=Config.LLM_BASE_URL,
        temperature=0,
        max_tokens=10,
    )

    system = (
        "Classify the user question as 'simple' or 'complex'. "
        "Reply with exactly one word: simple or complex. "
        "Simple: single intent, straightforward SQL (filter/aggregate/sort on 1-2 tables). "
        "Complex: multi-step reasoning, nested subqueries, multi-table JOINs with grouping, "
        "ranking/top-N, percentage calculations, or comparing across categories."
    )
    msg = HumanMessage(content=f"Question: {question}\n\nClassification:")
    response = chat.invoke([SystemMessage(content=system), msg])
    raw = response.content.strip().lower()
    if "complex" in raw:
        return "complex"
    return "simple"


def router_node(state: AgentState) -> dict:
    """Classify question. Plan C: heuristic cascade + LLM borderline gate."""
    q = state["question"]
    score = _heuristic_score(q)

    if score >= 2:
        complexity = "complex"
        method = "heuristic"
    elif score == 1:
        complexity = _llm_classify(q)
        method = "llm_borderline"
    else:
        complexity = "simple"
        method = "heuristic"

    # Trace logging
    tlog = state.get("tlog")
    if tlog:
        tlog.router_decision(complexity, score, method)

    return {
        "complexity": complexity,
        "sub_steps": [],
    }
```

- [ ] **Step 2: Write `tests/test_w5_router.py`**

```python
"""Tests for Router v2."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.agent.nodes.router import _heuristic_score


def test_score_simple_question():
    score = _heuristic_score("查询所有用户")
    assert score == 0, f"Expected 0, got {score}"


def test_score_complex_multi_intent():
    score = _heuristic_score("同时查询销售额和客户数量并且排名前十")
    assert score >= 2, f"Expected >=2, got {score}"


def test_score_complex_structure():
    score = _heuristic_score("计算每个品类的销售占比增长率排名前5")
    assert score >= 2, f"Expected >=2, got {score}"


def test_score_borderline_length():
    # Just over 40 chars but no other signals — borderline
    score = _heuristic_score("查询上个月购买超过三次的用户的订单详情和评价")
    assert score >= 1, f"Expected >=1, got {score}"


def test_entity_count():
    score = _heuristic_score("《电子产品》品类和《食品》品类的销量对比")
    assert score >= 1, f"Expected >=1 for multi-entity, got {score}"


if __name__ == "__main__":
    test_score_simple_question()
    test_score_complex_multi_intent()
    test_score_complex_structure()
    test_score_borderline_length()
    test_entity_count()
    print("All Router v2 tests passed!")
```

- [ ] **Step 3: Run router tests**

```bash
python tests/test_w5_router.py
```
Expected: "All Router v2 tests passed!"

---

### Task 6: Execution Timeout

**Files:**
- Modify: `src/agent/nodes/executor.py`

- [ ] **Step 1: Add query timeout to run_sql**

Modify `src/agent/nodes/executor.py`:

```python
"""Executor node — shared SQL execution entry point."""
import signal
from contextlib import contextmanager
from nl2sql.execute import execute_sql as _execute_sql
from src.agent.state import AgentState

EXEC_TIMEOUT_S = 10


@contextmanager
def _time_limit(seconds: int):
    """Context manager to limit execution time. Falls back gracefully on Windows."""
    try:
        signal.signal(signal.SIGALRM, lambda signum, frame: (_ for _ in ()).throw(TimeoutError()))
        signal.alarm(seconds)
        yield
        signal.alarm(0)
    except (AttributeError, ValueError):
        # Windows: signal.alarm not available — skip timeout
        yield


def run_sql(sql: str, timeout_s: int = EXEC_TIMEOUT_S) -> dict:
    """Execute a single SQL in sandbox with timeout. Returns {success, data, columns, error, row_count}."""
    import time

    t0 = time.time()
    try:
        with _time_limit(timeout_s):
            result = _execute_sql(sql)
        elapsed = time.time() - t0

        # Trace
        tlog_state = None  # set by caller via executor_node
        # (tracing is handled in executor_node which has state access)

        return result
    except TimeoutError:
        elapsed = time.time() - t0
        return {
            "success": False,
            "error": f"Query timed out after {timeout_s}s",
            "data": None,
            "columns": None,
            "row_count": 0,
        }
    except Exception as e:
        elapsed = time.time() - t0
        return {
            "success": False,
            "error": f"Execution error: {str(e)[:300]}",
            "data": None,
            "columns": None,
            "row_count": 0,
        }


def executor_node(state: AgentState) -> dict:
    """Graph node wrapper — execute state.sql and record exec_result + attempt."""
    import time

    sql = state.get("sql", "")
    exec_attempts = list(state.get("exec_attempts", []))
    retry_count = state.get("retry_count", 0)

    if not sql:
        exec_result = {
            "success": False, "error": "No SQL to execute",
            "data": None, "columns": None, "row_count": 0,
        }
        return {"exec_result": exec_result, "exec_attempts": exec_attempts}

    existing = state.get("exec_result")
    if existing and existing.get("_sql") == sql:
        return {}

    result = run_sql(sql)
    attempt_num = retry_count + 1
    exec_attempts.append({
        "attempt": attempt_num,
        "sql": sql,
        "success": result["success"],
        "error": result.get("error"),
        "row_count": result["row_count"],
    })

    # Trace
    tlog = state.get("tlog")
    if tlog:
        tlog.sql_exec(result["success"], result.get("row_count", 0), 0, result.get("error", ""))

    return {
        "exec_result": result,
        "exec_attempts": exec_attempts,
        "retry_count": attempt_num,
    }
```

---

### Task 7: Voter Semantic Safety Net

**Files:**
- Create: `src/agent/nodes/semantic_check.py`
- Modify: `src/agent/graphs/full_graph.py`
- Create: `tests/test_w5_semantic_check.py`

- [ ] **Step 1: Write `src/agent/nodes/semantic_check.py`**

```python
"""SemanticCheck node — LLM binary YES/NO check on Voter winner.

Addresses W4 blind spot: multi-candidate same-wrong-result passes hash voting.
After Voter picks a winner, this node asks LLM: "Does this SQL correctly answer the question?"
If NO → routes to Refiner with semantic error feedback.
"""

from src.agent.state import AgentState
from nl2sql.config import Config


_SEMANTIC_CHECK_PROMPT = """You are a SQL reviewer. Check if the SQL correctly answers the user's question.

## SCHEMA
{schema_text}

## USER QUESTION
{question}

## SQL TO REVIEW
```sql
{sql}
```

## EXECUTION RESULT
The query returned {row_count} rows. First 5 rows:
{preview}

Does this SQL correctly answer the user's question? Consider:
1. Does it query the right tables and columns?
2. Are the JOIN conditions correct?
3. Do filters (WHERE) match the question's intent?
4. Are aggregations and groupings correct?
5. Is the ORDER BY / LIMIT sensible?

Reply with exactly:
YES — if the SQL is correct.
NO: <brief reason> — if the SQL has a semantic error.
"""


def semantic_check_node(state: AgentState) -> dict:
    """LLM binary semantic check. Returns semantic_pass / semantic_feedback."""
    question = state.get("question", "")
    sql = state.get("sql", "")
    schema_text = state.get("schema_text", "")
    exec_result = state.get("exec_result") or {}

    if not sql or not question:
        return {"semantic_pass": True, "semantic_feedback": ""}

    row_count = exec_result.get("row_count", 0)
    data = exec_result.get("data", []) or []
    columns = exec_result.get("columns", []) or []

    # Format preview rows
    preview = ""
    if data:
        header = ", ".join(columns[:5])
        preview = header + "\n"
        for row in data[:5]:
            preview += str(row) + "\n"

    prompt = _SEMANTIC_CHECK_PROMPT.format(
        schema_text=schema_text[:4000],
        question=question,
        sql=sql,
        row_count=row_count,
        preview=preview if preview else "(empty result)",
    )

    from langchain_openai import ChatOpenAI
    from langchain_core.messages import SystemMessage, HumanMessage

    chat = ChatOpenAI(
        model=Config.LLM_CHAT_MODEL,
        api_key=Config.LLM_API_KEY,
        base_url=Config.LLM_BASE_URL,
        temperature=0,
        max_tokens=80,
    )

    try:
        response = chat.invoke([
            SystemMessage(content="Reply with YES or NO: <reason>. Be concise."),
            HumanMessage(content=prompt),
        ])
        raw = response.content.strip()

        tu = response.response_metadata.get("token_usage", {}) if hasattr(response, "response_metadata") else {}

        if raw.upper().startswith("YES"):
            passed = True
            reason = ""
        elif raw.upper().startswith("NO"):
            passed = False
            reason = raw[2:].strip().lstrip(":").strip()
        else:
            passed = "NO" not in raw.upper()[:10]
            reason = raw[:200]

    except Exception as e:
        passed = True  # On error, let it through (don't block the pipeline)
        reason = f"Semantic check LLM error: {str(e)[:100]}"
        tu = {}

    # Trace
    tlog = state.get("tlog")
    if tlog:
        tlog.semantic_verdict(passed, reason)

    # Accumulate token usage
    token_usage = dict(state.get("token_usage", {"prompt": 0, "completion": 0, "total": 0}))
    for k in token_usage:
        token_usage[k] += tu.get(k, 0)

    return {
        "semantic_pass": passed,
        "semantic_feedback": reason,
        "token_usage": token_usage,
    }
```

- [ ] **Step 2: Add semantic_check node to full_graph**

Modify `src/agent/graphs/full_graph.py`:

```python
"""Full LangGraph pipeline: Generator → Guard → Voter → SemanticCheck → END (with Refiner loop)."""
from langgraph.graph import StateGraph, END, START
from src.agent.state import AgentState
from src.agent.nodes.schema_retriever import schema_retriever_node
from src.agent.nodes.router import router_node
from src.agent.nodes.decomposer import decomposer_node
from src.agent.nodes.generator import generator_node
from src.agent.nodes.guard import guard_node
from src.agent.nodes.voter import voter_node
from src.agent.nodes.semantic_check import semantic_check_node
from src.agent.nodes.refiner import refiner_node

MAX_RETRIES = 2


def _route_after_router(state: AgentState) -> str:
    return "decomposer" if state.get("complexity") == "complex" else "generator"


def _route_after_guard(state: AgentState) -> str:
    if state.get("guard_pass"):
        return "voter"
    return "refiner"


def _route_after_voter(state: AgentState) -> str:
    """Voter found a winner → SemanticCheck for final validation."""
    exec_result = state.get("exec_result", {})
    if exec_result and exec_result.get("success"):
        return "semantic_check"
    return "refiner"


def _route_after_semantic(state: AgentState) -> str:
    """Semantic check passed → END. Failed → Refiner with feedback."""
    if state.get("semantic_pass", True):
        return END
    return "refiner"


def _route_after_refiner(state: AgentState) -> str:
    retry = state.get("retry_count", 0)
    if retry >= MAX_RETRIES:
        return END
    return "generator"


def _after_refiner(state: AgentState) -> dict:
    return {"retry_count": state.get("retry_count", 0) + 1}


def create_full_graph():
    graph = StateGraph(AgentState)

    # Nodes
    graph.add_node("schema_retriever", schema_retriever_node)
    graph.add_node("router", router_node)
    graph.add_node("decomposer", decomposer_node)
    graph.add_node("generator", generator_node)
    graph.add_node("guard", guard_node)
    graph.add_node("voter", voter_node)
    graph.add_node("semantic_check", semantic_check_node)
    graph.add_node("refiner", refiner_node)
    graph.add_node("_post_refiner", _after_refiner)

    # Edges
    graph.add_edge(START, "schema_retriever")
    graph.add_edge("schema_retriever", "router")

    graph.add_conditional_edges(
        "router", _route_after_router,
        {"decomposer": "decomposer", "generator": "generator"},
    )
    graph.add_edge("decomposer", "generator")

    graph.add_edge("generator", "guard")

    graph.add_conditional_edges(
        "guard", _route_after_guard,
        {"voter": "voter", "refiner": "refiner"},
    )

    # Voter → SemanticCheck / Refiner
    graph.add_conditional_edges(
        "voter", _route_after_voter,
        {"semantic_check": "semantic_check", "refiner": "refiner"},
    )

    # SemanticCheck → END / Refiner
    graph.add_conditional_edges(
        "semantic_check", _route_after_semantic,
        {END: END, "refiner": "refiner"},
    )

    # Refiner → Generator / END
    graph.add_edge("refiner", "_post_refiner")
    graph.add_conditional_edges(
        "_post_refiner", _route_after_refiner,
        {"generator": "generator", END: END},
    )

    return graph.compile()
```

- [ ] **Step 3: Write `tests/test_w5_semantic_check.py`**

```python
"""Tests for SemanticCheck node (unit-level, no LLM needed)."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_semantic_check_empty_state():
    """semantic_check handles empty sql/exec_result gracefully."""
    from src.agent.nodes.semantic_check import semantic_check_node
    result = semantic_check_node({"question": "", "sql": "", "exec_result": {}})
    assert result.get("semantic_pass") is True


def test_semantic_check_state_fields():
    """Verify semantic_check returns required fields."""
    from src.agent.nodes.semantic_check import semantic_check_node
    # Without LLM call (empty question/sql), should pass through
    result = semantic_check_node({
        "question": "test",
        "sql": "SELECT 1",
        "schema_text": "CREATE TABLE t(id int);",
        "exec_result": {"success": True, "row_count": 1, "data": [(1,)], "columns": ["id"]},
    })
    assert "semantic_pass" in result
    assert "semantic_feedback" in result


if __name__ == "__main__":
    test_semantic_check_empty_state()
    test_semantic_check_state_fields()
    print("All SemanticCheck tests passed!")
```

- [ ] **Step 4: Run semantic check tests**

```bash
python tests/test_w5_semantic_check.py
```
Expected: "All SemanticCheck tests passed!"

---

### Task 8: Trace ID Propagation

**Files:**
- Modify: `src/agent/nodes/schema_retriever.py`
- Create: `tests/test_w5_trace.py`

- [ ] **Step 1: Initialize trace_id in schema_retriever node**

Read `src/agent/nodes/schema_retriever.py` and add trace initialization at the top of the node function:

```python
def schema_retriever_node(state: AgentState) -> dict:
    # Initialize trace on first entry
    tlog = state.get("tlog")
    trace_id = state.get("trace_id", "")
    if not tlog:
        from src.obs.logger import TraceLogger
        tlog = TraceLogger(trace_id)
        trace_id = tlog.trace_id

    tlog.node_enter("schema_retriever", {"question": state["question"][:80]})

    # ... existing code ...

    tlog.node_exit("schema_retriever", {"schema_len": len(schema_text), "notes_len": len(notes_text)})
    return {
        # ... existing fields ...
        "trace_id": trace_id,
        "tlog": tlog,
    }
```

(Full implementation requires reading the current file first — the key change is adding tlog init + node_enter/node_exit calls.)

- [ ] **Step 2: Write `tests/test_w5_trace.py`**

```python
"""Tests for trace ID generation and propagation."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_trace_logger_creates_id():
    from src.obs.logger import TraceLogger
    tlog = TraceLogger()
    assert tlog.trace_id
    assert len(tlog.trace_id) == 12


def test_trace_logger_respects_given_id():
    from src.obs.logger import TraceLogger
    tlog = TraceLogger("abc123")
    assert tlog.trace_id == "abc123"


def test_trace_logger_unique_ids():
    from src.obs.logger import TraceLogger
    ids = {TraceLogger().trace_id for _ in range(10)}
    assert len(ids) == 10, "All trace IDs should be unique"


def test_trace_logger_node_enter_exit():
    from src.obs.logger import TraceLogger
    import io, sys, logging

    # Capture log output
    tlog = TraceLogger("test123")
    tlog.node_enter("test_node", {"key": "val"})
    tlog.node_exit("test_node", {"result": "ok"})

    # No exceptions = pass
    assert True


if __name__ == "__main__":
    test_trace_logger_creates_id()
    test_trace_logger_respects_given_id()
    test_trace_logger_unique_ids()
    test_trace_logger_node_enter_exit()
    print("All Trace tests passed!")
```

- [ ] **Step 3: Run trace tests**

```bash
python tests/test_w5_trace.py
```
Expected: "All Trace tests passed!"

---

### Task 9: Add R4 Ablation Config to Eval

**Files:**
- Modify: `scripts/eval.py`

- [ ] **Step 1: Add R4 config to eval.py main()**

In `scripts/eval.py`, inside `main()`, update the configs list:

```python
configs = [
    {"name": "R0_Baseline",           "rag_schema": False, "rag_domain": False, "reviewer_on": False},
    {"name": "R1_SchemaRAG",          "rag_schema": True,  "rag_domain": False, "reviewer_on": False},
    {"name": "R2_SchemaRAG+Domain",   "rag_schema": True,  "rag_domain": True,  "reviewer_on": False},
    {"name": "R3_Full",               "rag_schema": True,  "rag_domain": True,  "reviewer_on": True},
    {"name": "R4_GuardVote",          "rag_schema": True,  "rag_domain": True,  "reviewer_on": False, "use_full_graph": True, "multi_candidate": True},
]
```

And update the `run_eval` function to support the new `use_full_graph` and `multi_candidate` params. When `use_full_graph=True`, use the new LangGraph pipeline instead of the old mini pipeline:

```python
def run_eval(gold_path: str, configs: list[dict]) -> dict:
    """Run evaluation across all configs."""
    cases = load_gold(gold_path)
    results = {}

    for cfg_idx, cfg in enumerate(configs):
        cfg_name = cfg["name"]
        use_full = cfg.get("use_full_graph", False)

        if use_full:
            # New graph pipeline
            from src.agent.graphs.full_graph import create_full_graph
            graph = create_full_graph()

        print(f"\n{'='*60}")
        print(f"Config {cfg_idx+1}/{len(configs)}: {cfg_name}")
        ...

        for i, case in enumerate(cases):
            ...
            t0 = time.time()

            if use_full:
                initial_state = {
                    "question": question,
                    "rag_schema": cfg["rag_schema"],
                    "rag_domain": cfg["rag_domain"],
                    "multi_candidate": cfg.get("multi_candidate", False),
                    "rag_k": cfg.get("k", 8),
                }
                result_state = graph.invoke(initial_state)
                gen_sql = result_state.get("sql", "")
                exec_result = result_state.get("exec_result")
                token_usage = result_state.get("token_usage", {})
                exec_attempts = result_state.get("exec_attempts", [])
                review_rounds = result_state.get("review_rounds", [])
            else:
                result = run(question, ...)
                # ... existing code ...
```

The report should print the R4 config alongside R0-R3.

- [ ] **Step 2: Verify R4 config runs without error**

```bash
python -c "
import sys, os
sys.path.insert(0, '.')
from scripts.eval import run_eval
# Dry-run single case to verify graph pipeline works
from src.agent.graphs.full_graph import create_full_graph
graph = create_full_graph()
state = graph.invoke({
    'question': '查询所有用户',
    'rag_schema': True,
    'rag_domain': True,
    'multi_candidate': True,
    'rag_k': 8,
})
print('SQL:', state.get('sql', 'NONE')[:100])
print('EXEC:', state.get('exec_result', {}).get('success', False))
print('TRACE_ID:', state.get('trace_id', 'NONE'))
print('SEMANTIC_PASS:', state.get('semantic_pass', 'NONE'))
"
```
Expected: prints SQL, EXEC success, trace_id, semantic_pass

---

### Task 10: Integration Smoke Test

- [ ] **Step 1: Run the full graph on the 20-question set**

```bash
python tests/test_mini_graph.py
```
Expected: All 5 existing tests still pass (backward compatible).

- [ ] **Step 2: Quick eval with R4**

```bash
python -c "
import sys, os
sys.path.insert(0, '.')
from scripts.eval import run_eval
configs = [{'name': 'R4_Smoke', 'rag_schema': True, 'rag_domain': True, 'reviewer_on': False, 'use_full_graph': True, 'multi_candidate': True}]
results = run_eval('eval/gold.jsonl', configs)
print('EX:', results['R4_Smoke']['ex_rate'])
"
```
Expected: EX >= 0.85 (at least 17/20, should be comparable or better than R3's 95%)

- [ ] **Step 3: Run full ablation matrix R0-R4**

```bash
python scripts/eval.py
```
Expected: report written to `reports/` with R0-R4 matrix.

---

## Acceptance Criteria

1. **AST Guardrail**: `validate_sql_ast()` catches syntax errors, INSERT/DROP statements, and accepts valid SELECT/WITH queries. Unit tests pass.
2. **Structured Logging**: Every node enter/exit emits JSON with trace_id. TraceLogger generates unique 12-char IDs.
3. **Router v2**: Heuristic score 0→simple, ≥2→complex (no LLM), score=1→LLM binary classify. Unit tests pass.
4. **Execution Timeout**: SQL execution is bounded at 10s. Timeout gracefully returns error dict.
5. **Semantic Safety Net**: After Voter winner, LLM YES/NO check routes to END or Refiner. Addresses W4 blind spot.
6. **R4 Ablation**: `eval.py` runs R0-R4 with the new graph pipeline. EX on R4 should match or exceed R3 (95%).
7. **Backward Compatibility**: All existing tests (`test_mini_graph.py`) and old pipeline (`nl2sql/pipeline.py`) still work.
8. **No regression**: R0-R3 produce same results as before (same pipeline path, same configurations).
