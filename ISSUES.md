# 问题记录

> 项目：NL2SQL Mini Agent | 起始日期：2026-05-09

---

## 1. SQL 文件被思考文本污染

**现象**：`seed.sql` 执行时报 `Parse error near "Actually": syntax error`

**原因**：Write 工具写入文件时，内部分析文本泄露进了 SQL 内容。

**解决**：重写整个 `seed.sql`，确保只包含有效 SQL，不含任何注释外的文本。

---

## 2. SQLAlchemy 2.0 `engine.execute()` 已移除

**现象**：`AttributeError: 'Engine' object has no attribute 'execute'`

**原因**：SQLAlchemy 2.0 移除了 `Engine.execute()`，必须使用连接上下文。

**解决**：
```python
# 旧写法
result = engine.execute(text(sql))

# 新写法
with engine.connect() as conn:
    result = conn.execute(text(sql))
```

---

## 3. `inspect.get_pk_constraint()` 返回值结构误解

**现象**：`TypeError: string indices must be integers, not 'str'`

**原因**：`get_pk_constraint()` 返回 `{'constrained_columns': ['col1'], 'name': None}`，`constrained_columns` 的值是字符串列表，不是字典列表。错误地对每个元素用了 `c["name"]`。

**解决**：
```python
# 错误
pk_cols = {c["name"] for c in pk.get("constrained_columns", [])}

# 正确
pk_cols = set(pk.get("constrained_columns", []))
```

---

## 4. DDL 生成中外键约束缺逗号

**现象**：生成的 DDL 中最后一个列定义和外键约束之间没有逗号分隔。

**原因**：列定义和外键约束分别用两次 `append` 添加到 `lines`，之间无连接符。

**解决**：合并为单一的 `members` 列表，列和 FK 统一放入，用 `",\n".join(members)` 一次性输出。

---

## 5. LangChain 新版导入路径变更

**现象**：`ModuleNotFoundError: No module named 'langchain.schema'`

**原因**：LangChain 1.2+ 中 `SystemMessage` / `HumanMessage` 移到 `langchain_core.messages`。

**解决**：
```python
# 旧
from langchain.schema import SystemMessage, HumanMessage

# 新
from langchain_core.messages import SystemMessage, HumanMessage
```

---

## 6. Bash 内联 Python 的转义冲突

**现象**：`-c` 内联 Python 代码中 SQL 字符串含双引号、换行符，与 bash 解析冲突。

**解决**：改用独立 `.py` 测试脚本文件，避免 shell 转义问题。

---

## 7. Python GBK 编码读取 UTF-8 文件失败

**现象**：`UnicodeDecodeError: 'gbk' codec can't decode byte 0xaf`

**原因**：Windows 下 Python 默认使用 GBK 编码打开文件。

**解决**：`open(path, encoding='utf-8')` 显式指定编码。

---

## 8. Voter 执行投票强依赖数据库连接

**现象**：Voter 通过执行候选 SQL 到真实数据库做 hash 投票。当 `database_url` 不可用时，整个 Voter 节点崩溃，多候选管线退化失败。

**分析**：
- 业界有两条替代路线：LLM 文本投票（DIN-SQL/DAIL-SQL）、EXPLAIN 计划比对
- 生产 NL2SQL 通常有数据库，但防御性设计应提供 fallback

**建议方案（优先级低，待排期）**：
```python
# voter_node 入口加 fallback 检查
if not database_url:
    return _llm_vote(candidates, question, schema)  # LLM 直接评
else:
    return _exec_vote(candidates, database_url)       # 当前逻辑（执行投票）
```

**状态**：✅ 已修复 (2026-05-21)

**方案**：`voter.py` 新增 `_llm_vote()` — 无 DB 时直接走 LLM 评选；全部候选超时 → LLM fallback 选最优而非报错进 Refiner。同时候选执行改为 ThreadPoolExecutor 并行，最坏耗时从 3×10s 降到 max 10s。

---

## 9. Guard 幻觉检测误判 SQL 注释中的标识符

**现象**：`#701` 生成 SQL 的 Guard 检查报 `hallucination: "Identifier 'HIGHEST' not in schema."`，但 "HIGHEST" 仅在 SQL 注释中出现（`-- Step 1: Identify the most influential user (highest reputation)`），SQL 代码本身无此标识符。

**原因**：Guard 的 hallucination 检测 regex 扫描范围覆盖了 SQL 注释（`-- ...`），将自然语言词汇误认为 SQL 标识符。

**影响**：误判导致 SQL 进入 Refiner → Generator 重试循环，即使 SQL 本身正确。在 max_retries 较低时可能直接拒绝正确 SQL。

**解决**：`src/agent/nodes/guard.py:_check_hallucinations()` 在提取标识符前先 strip 注释：
```python
sql_clean = re.sub(r"--[^\n]*", "", sql_clean)          # strip single-line comments
sql_clean = re.sub(r"/\*.*?\*/", "", sql_clean, flags=re.DOTALL)  # strip multi-line comments
```

**状态**：✅ 已修复 (2026-05-21)

---

## 10. Windows 反斜杠数据库路径导致 SQLAlchemy 挂起

**现象**：`get_database_url()` 在 Windows 上返回 `sqlite:///F:\path\to\db.sqlite`（含反斜杠），多线程并发执行 SQL 时 SQLAlchemy 挂起无响应。

**原因**：SQLAlchemy 的 URL 解析器期望正斜杠。反斜杠在连接字符串中被误解析，导致引擎无法正确连接数据库文件。

**解决**：
```python
# bird_loader.py — get_database_url()
def get_database_url(sample: BirdSample) -> str:
    return f"sqlite:///{sample.database_path.replace(chr(92), '/')}"
```

---

## 11. ThreadPoolExecutor shutdown 在子线程卡死时阻塞

**现象**：`_precompute_gold.py` 中某个 worker 线程卡在 `execute_sql()`（慢 SQL 如 #701 需 864s），即使主线程已通过 `FutureTimeoutError` 放弃等待，`with ThreadPoolExecutor` 的 `__exit__` 仍阻塞在 `shutdown(wait=True)`，导致整个脚本无法退出。

**原因**：`ThreadPoolExecutor.__exit__` 默认调用 `shutdown(wait=True)`，会等待所有已提交的 worker 完成。被超时放弃的 worker 线程仍在执行 SQL，shutdown 永远等不到它。

**解决**：
```python
# 显式 pool.shutdown(wait=False)，不阻塞在 stuck 线程上
pool = ThreadPoolExecutor(max_workers=1)
try:
    fut = pool.submit(_exec)
    return fut.result(timeout=timeout_s)
except FutureTimeoutError:
    return {"status": "timeout", ...}
finally:
    pool.shutdown(wait=False)  # 关键：不等 stuck 线程
```

---

## 12. SQLAlchemy 误解析 LIKE 模式为 bind parameters

**现象**：`formula_1` 数据库 3 道题（#959, #989, #990）的 gold SQL 执行报错：
```
InvalidRequestError: A value is required for bind parameter '__'
```

**原因**：gold SQL 中包含 `LIKE '_:%:__.___'` 这类模式匹配字符串。SQLAlchemy 的 `text()` 将 `:__` 和 `:___` 解析为命名 bind parameter，要求调用方提供值。

**解决**：在 `_precompute_gold.py` 中添加 raw sqlite3 fallback。当 SQLAlchemy 报 `bind parameter` 错误时，直接使用 `sqlite3` 标准库执行：
```python
if "bind parameter" in err.lower():
    import sqlite3
    conn = sqlite3.connect(db_path)
    cur = conn.execute(sample.gold_sql)
    rows = cur.fetchall()
    ...
```

---

## 13. BIRD Mini-Dev JSON 存在重复题目

**现象**：`financial` 数据库的 gold JSON 包含 500 条记录，但只有 498 个唯一 question_id。#137 和 #138 各出现两次，内容完全一致。

**验证**：
```python
import json
with open("financial.json") as f:
    data = json.load(f)
# len(data) = 500
# len({s["question_id"] for s in data}) = 498
# duplicates: #137, #138
```

**结论**：BIRD 数据集上游问题，不影响评测（重复题目执行结果相同）。加载时按 question_id 自然去重。

**状态**：已知问题，无需修复（上游数据集 bug）

---

## 14. 部分 BIRD gold SQL 自身执行极慢

**现象**：
- #701 (codebase_community): gold SQL 执行需 864s（14.4 分钟）— `users` (40,325 行) FULL JOIN `posts` (91,966 行)，无索引
- #518 (card_games): gold SQL 执行需 180s — 大表全表扫描

**原因**：BIRD 的 gold SQL 标注侧重语义正确性，不保证执行效率。SQLite 数据库均为裸表，无索引、无预计算物化视图。

**影响**：每次 eval 重新执行 gold SQL 时，这些题会阻塞整个评测 pipeline（线程池被占满，其他题排队等待）。

**解决方案**：
1. `scripts/_precompute_gold.py` — 一次性预计算所有 gold SQL 结果，存入 `reports/.gold_cache/<db_id>.json`
2. `src/eval/metrics.py` — `exec_match()` 新增 `gold_cache` 参数，命中缓存时跳过 gold SQL 执行，只执行 gen SQL
3. `scripts/eval_bird.py` — `run_bird_eval()` 启动时加载全部 cache，传给每个 worker

**状态**：已解决（gold cache 系统）
