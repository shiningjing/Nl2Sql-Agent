# CLAUDE.md — NL2SQL Agent v0.2.3 → DataAgentOps

## 项目概览

自然语言 → SQL 端到端系统。LangGraph 状态机编排（Router → Schema Retriever → Decomposer → Generator → Guard → Voter → SemCheck → Refiner）。BIRD Mini-Dev（500 题，11 DB，3 方言）消融评测。

**BIRD 结果**：DeepSeek V4 Pro（日常）EX **38.8%** / Claude Opus 4.7 EX **47.0%**。RAG Table Recall 95.9%。

**核心发现**：RAG 最大杠杆 (+11pp)；Decomposer 对 DeepSeek 无效；换强模型 +8pp；Self-Correction 修复率 7-20% 仍是瓶颈。

## DataAgentOps 升级计划（2026-06 启动，4 周）

> 详细计划文档：`docs/DataAgentOps升级计划.md`

**目标**：从单体应用提升为可观测、可评测、可扩展、可部署的 Data Agent 平台。不以提精度为首要目标，BIRD EX 不低于 DeepSeek 基线（38.8%）。

### 目标架构

```
Web UI / API Client → FastAPI Gateway ──SSE──→ Kafka → LangGraph Agent Worker
                                                              │
                                                      MCP Tool Layer
                                                              │
                                              Schema Service / SQL Validator
                                              SQL Executor / Domain Knowledge
                                                              │
                                              PostgreSQL / ChromaDB / Redis

OpenTelemetry → Tracing / Metrics / Logs
```

### 四周安排

| 周次  | 主题               | 核心交付                                                      |
| --- | ---------------- | --------------------------------------------------------- |
| W1  | 重构 + AgentOps 基础 | 模块化拆分、统一 AgentState、OpenTelemetry 全链路 tracing、BIRD 自动评测脚本 |
| W2  | MCP 工具 + SQL 安全    | 2 个 MCP 工具（validate_sql + execute_readonly_sql）、SQL 安全层（9 规则）、统一错误分类     |
| W3  | 异步任务 + SSE       | Kafka (4 Topic) + Redis 任务状态机、SSE 流式接口、重试/幂等/超时/取消        |
| W4  | 部署 + 压测 + 文档     | Docker Compose 一键启动、K8s 部署、三类压测（功能/性能/稳定性）、简历材料           |

### 优先级

- **P0**：模块化重构、自动评测、OTel tracing、MCP 工具层、SQL 安全、Kafka 异步、Redis 状态、SSE、Docker Compose、README
- **P1**：K8s、Dead-letter queue、任务取消、prompt 版本管理、失败回放、Grafana
- **P2**：Go 工具网关、gRPC、多租户、自动扩缩容、混合检索、成本治理

## W2 执行计划：MCP 工具 + SQL 安全层 + 统一错误分类 + Voter 优化

**目标**：交付 2 个可独立调用的 MCP 工具，补齐 SQL 安全层，规范错误分类，优化 Voter 策略。

### 任务 1：MCP 工具 `validate_sql`（0.5 天）

用 Python `fastmcp` 实现，输入 SQL 字符串 + 方言，输出 `{valid, issues[]}`。

**现有代码基础**：`guard/ast_validator.py` 已有 sqlglot 校验（语法、禁止类型、多语句检测），直接复用。

**输入**：
```json
{"sql": "SELECT ...", "dialect": "sqlite"}
```

**输出**：
```json
{
  "valid": false,
  "issues": [
    {"type": "ast_syntax", "detail": "SQL parse error: ..."},
    {"type": "ast_forbidden", "detail": "Forbidden statement type: INSERT."},
    {"type": "ast_structure", "detail": "Multiple statements detected (3)."}
  ],
  "statement_type": "SELECT",
  "table_references": ["orders", "customers"]
}
```

**子任务**：
- 1.1 安装 `fastmcp`（`pip install fastmcp`）
- 1.2 写 `tools/mcp/validate_sql_server.py` — 包装 `guard/ast_validator.py` 的校验逻辑
- 1.3 写 `tools/mcp/__init__.py` — MCP 工具目录
- 1.4 写 `tests/test_validate_sql_mcp.py` — MCP 客户端测试（合法 SELECT / 多语句 / DROP / 语法错误）

### 任务 2：MCP 工具 `execute_readonly_sql`（1 天）

MCP 工具独立进程执行 SQL，硬限制：只读、超时、行数上限、内存限制。

**现有代码基础**：`tools/sql_executor.py` 已有基础执行逻辑，需要加固安全边界。

**输入**：
```json
{
  "sql": "SELECT * FROM orders",
  "database_url": "sqlite:///data/bird/...",
  "max_rows": 200,
  "timeout_ms": 30000
}
```

**输出**：
```json
{
  "success": false,
  "error": "Query timeout after 30000ms",
  "error_type": "TIMEOUT",
  "data": null, "columns": null, "row_count": 0,
  "execution_ms": 30120
}
```

**安全硬限制**（在 MCP 工具层实现，不依赖调用方守规矩）：
- 只允许 SELECT/WITH（正则 + sqlglot AST 双重校验）
- 自动包装 LIMIT（sql_upper 中无 LIMIT 时自动加 `SELECT * FROM (...) LIMIT {max_rows}`）
- `StatementTimeout` 连接级超时
- `max_rows` 硬上限 1000（超过拒绝，不静默截断）
- 执行结果行数 ≤ max_rows

**子任务**：
- 2.1 写 `tools/mcp/execute_readonly_server.py` — MCP 工具主体
- 2.2 连接级超时控制（`statement_timeout` for PG, `max_statement_time` for MySQL, 线程超时 for SQLite）
- 2.3 加固安全检查（双重校验：正则关键词 + sqlglot AST）
- 2.4 写 `tests/test_execute_readonly_mcp.py` — 安全测试（SELECT / INSERT 拒绝 / 多语句 / LIMIT 自动包装）

### 任务 3：SQL 安全层 — 9 规则统一（1 天）

**问题**：当前安全检查分散在 3 个文件中：
- `tools/sql_executor.py:_safety_check()` — 正则关键词匹配
- `agent/nodes/guard.py:_validate_sql()` — 正则关键词匹配（与上面重复）
- `guard/ast_validator.py:validate_sql_ast()` — sqlglot AST 校验

**目标**：收敛到 `guard/safety_rules.py`，9 条规则统一入口。

```
Rule 1  DDL 拦截     — INSERT/UPDATE/DELETE/DROP/ALTER/CREATE/TRUNCATE/PRAGMA 拒绝
Rule 2  多语句拦截     — 分号分割后 > 1 条即拒绝
Rule 3  LIMIT 兜底   — 无 LIMIT 自动包装，上限 1000（超过拒绝）
Rule 4  查询超时      — 连接级 statement_timeout，默认 30s
Rule 5  敏感列过滤    — 检测 password/token/secret/api_key 等敏感列名，产出 warning
Rule 6  表/列存在性  — AST 提取表名列名 → 与 schema 交叉校验（硬规则，非 LLM）
Rule 7  WHERE 检测   — 无 WHERE 的 UPDATE/DELETE 已由 Rule 1 拦截；对 SELECT 产出 warning（全表扫描风险）
Rule 8  子查询深度    — sqlglot AST 遍历，嵌套 > 3 层拒绝
Rule 9  JOIN 表数    — FROM + JOIN 表数 > 6 拒绝
```

**子任务**：
- 3.1 写 `guard/safety_rules.py` — 9 条规则统一入口，输入 sql + dialect + schema_info，输出 `{passed, issues[], warnings[]}`
- 3.2 重构 `tools/sql_executor.py` — 删除 `_safety_check()`，改为调用 `guard/safety_rules.py`
- 3.3 重构 `agent/nodes/guard.py:_validate_sql()` — 同上
- 3.4 写 `tests/test_safety_rules.py` — 9 条规则各 2+ case

### 任务 4：统一错误分类（0.5 天）

**现有**：`guard/error_classifier.py:_classify_exec_error()` 已有 8 种错误类型。

**目标**：规范化为 10 种标准错误码，给 `tools/sql_executor.py`、MCP 工具、节点统一使用。

```python
class ErrorType(StrEnum):
    RETRIEVAL_ERROR = "retrieval_error"        # Schema/RAG 检索失败
    INVALID_SCHEMA = "invalid_schema"          # 指定的表/列不存在
    INVALID_COLUMN = "invalid_column"          # SQL 引用了不存在的列
    SQL_SYNTAX_ERROR = "sql_syntax_error"      # 语法解析失败
    SQL_EXECUTION_ERROR = "sql_execution_error" # 执行时数据库报错
    PERMISSION_DENIED = "permission_denied"    # 禁止的语句类型
    TIMEOUT = "timeout"                        # 查询超时
    EMPTY_RESULT = "empty_result"              # 查询返回 0 行
    SEMANTIC_MISMATCH = "semantic_mismatch"    # 语义校验不匹配
    TOOL_UNAVAILABLE = "tool_unavailable"      # MCP 工具不可用
```

**子任务**：
- 4.1 写 `guard/error_types.py` — ErrorType 枚举 + 错误码 → HTTP 状态码映射
- 4.2 重构 `guard/error_classifier.py` — 映射到 ErrorType
- 4.3 `tools/sql_executor.py` 错误返回带上 `error_type`

### 任务 5：Voter 按需激活 + 平票逻辑改进（0.5 天）

**问题**：当前 Generator 始终 3 temp 多候选 + Voter 投票，但 BIRD 100 抽测显示 95% 的题 3 个 temp 生成相同 SQL，多候选的 LLM 成本白花了。

**策略**：

```
正常路径（retry_count == 0）:
  Generator temp=0 单条 → Guard → Voter（仅执行，不投票）→ SemCheck → END

Self-Correction 路径（retry_count > 0）:
  Refiner → Generator 多候选(0/0.3/0.6) → Guard → Voter（真正投票）→ SemCheck → END/refiner
```

- Generator：`retry_count > 0` 时启用多候选+去重早停，否则单条 temp=0
- Voter：单候选直接执行返回（现有逻辑已支持），多候选才走投票

**平票逻辑改进**：3 个候选执行成功但 hash 各不相同（无多数派）时，当前选 `min(row_count)` 不合理。改为 LLM vote（带 schema + 执行结果 column/row_count），LLM vote 失败再 fallback 到候选 0。

**子任务**：
- 5.1 Generator 加 `retry_count > 0` 判断控制多候选开关
- 5.2 Voter tiebreak 分支：`min(row_count)` → LLM vote（带执行结果信息）
- 5.3 LLM vote prompt 增强：已有执行结果时附上 columns + row_count

### 任务 6：回归验证（0.5 天）

- 6.1 BIRD 100 题抽测 EX 无退化，确认多候选按需激活逻辑正常
- 6.2 验证 Voter 平票改进（查看 trace 中 tiebreak 走 LLM vote 的 case）
- 6.3 多数据库冒烟（SQLite 3 题，MySQL/PG Docker 可用时补上）
- 6.4 MCP 工具独立测试通过

### W2 时间依赖

```
任务 1 (validate_sql MCP) ──┬──▶ 任务 3 (SQL 安全层 9 规则)
       0.5 天               │           1 天
                             │              │
任务 2 (execute MCP) ────────┘              ▼
       1 天                          任务 4 (统一错误分类)
                                           0.5 天
                                             │
                                             ▼
                                      任务 5 (Voter 优化)
                                           0.5 天
                                             │
                                             ▼
                                      任务 6 (回归验证)
                                           0.5 天
总用时：3 天（任务 1+2 可并行）
```

### W2 交付物

- `tools/mcp/` — 2 个 MCP 工具 + 测试
- `guard/safety_rules.py` — 9 条安全规则统一入口
- `guard/error_types.py` — 10 种标准错误码
- Generator+Voter 按需激活 + 平票 LLM vote
- 重构后的 `tools/sql_executor.py` 和 `agent/nodes/guard.py`（删除分散的安全检查，统一走 safety_rules）
- BIRD 100 题抽测报告（对比 W2 前后）

### Go 技术栈切入点

- **MCP Server 用 Go 写**（P1）：`database/sql` 连接池管理、`vitess/sqlparser` AST 校验、`mark3labs/mcp-go` SDK，替换 Python MCP Server。Python MCP Client 无需改动。
- **API 网关**：Go `go-chi`/`grpc-gateway` 做限流、鉴权、协议转换
- **Schema 缓存服务**：Go 常驻内存缓存 + gRPC，替代 Python `lru_cache`

## 技术栈

| 层         | 当前                                   | 升级后新增                       |
| --------- | ------------------------------------ | --------------------------- |
| LLM       | DeepSeek / OpenAI / Claude           | —                           |
| Embedding | BAAI/bge-small-zh-v1.5 (本地)          | —                           |
| 向量库       | ChromaDB 本地                          | —                           |
| 编排        | LangGraph + LangChain 1.2+           | —                           |
| DB        | SQLite / PG / MySQL (SQLAlchemy 2.0) | —                           |
| AST       | sqlglot                              | + vitess/sqlparser (Go MCP) |
| 接口        | FastAPI + Streamlit                  | + SSE 流式                    |
| 缓存        | Redis (本地优先)                         | + Redis 任务状态                |
| 消息        | —                                    | + Kafka                     |
| 可观测       | TraceLogger (jsonl)                  | + OpenTelemetry             |
| 工具        | —                                    | + MCP 协议                    |
| 部署        | Docker Compose                       | + K8s                       |
| 网关        | —                                    | + Go (P1)                   |

## 关键约束

1. SQL 只允许 SELECT（含 WITH），DML/DDL 一律拒绝
2. Schema 消费者统一走 `_get_cached_schema_info()`，禁止 raw `inspect()`
3. Generator 默认 temperature=0 单条；仅 Self-Correction 时启用多候选 0/0.3/0.6 + 去重早停
4. LIMIT 默认 200，硬上限 1000
5. SQLite 自动 `PRAGMA journal_mode=WAL`
6. ThreadPoolExecutor 超时后 `pool.shutdown(wait=False)`
7. 所有 prompt 在 `src/prompts.py`

## 项目结构

```
nl2sql-mini-agent/
  nl2sql/                  # 核心库 (schema, execute, generate, db_registry, rag_retrieve, config, review)
  src/
    agent/                 # LangGraph 节点 (router, schema_retriever, decomposer, generator, guard, voter, executor, semantic_check, refiner)
      state.py             # AgentState TypedDict
      graphs/full_graph.py # 主图（条件边+循环）
    eval/                  # BIRD 评测 (bird_loader, metrics, task_manager)
    prompts.py             # 所有 LLM prompt
    retrieval/             # RAG 管线 (fk_expand, column_prune, fewshot)
    api/                   # FastAPI
    infrastructure/        # Redis 缓存 + LLM 工厂
    obs/                   # TraceLogger
    guardrails/            # sqlglot AST 校验
  corpus/bird_fewshot/     # Few-shot 示例（按 db_id + 方言）
  scripts/
    eval_bird.py           # 主评测（--test / --exp ablation）
    _precompute_gold.py    # Gold SQL 预计算缓存
    _smoke_multidb.py      # 多数据库冒烟测试
    ingest_bird.py         # BIRD schema 向量化
  reports/.gold_cache/     # Gold SQL 预计算结果
  logs/traces/             # 流式 trace (jsonl)
  data/bird/mini_dev_data/ # BIRD Mini-Dev 数据集
  llm_keys.json            # LLM 密钥（不提交 Git）
  databases.json           # 用户自定义数据库连接
```

## LLM 预设 (8 个)

密钥在 `llm_keys.json`：`{"deepseek": "", "openai": "", "anthropic": ""}`。自动检测 Anthropic URL/Model 映射到 `ChatAnthropic`。

| Provider                          | Model                                               |
| --------------------------------- | --------------------------------------------------- |
| DeepSeek V4 Pro / Chat / Reasoner | deepseek-v4-pro / deepseek-chat / deepseek-reasoner |
| OpenAI GPT-4o / GPT-4o-mini       | gpt-4o / gpt-4o-mini                                |
| Claude Opus 4.7 / Sonnet 4.6      | claude-opus-4-7 / claude-sonnet-4-6                 |
| Custom                            | 任意                                                  |

## 命令速查

```bash
# 评测（测试/完整消融/预计算 gold）
python scripts/eval_bird.py --test --samples 20 --configs R2,R5
python scripts/eval_bird.py --exp ablation --max-workers 8
python scripts/_precompute_gold.py

# 冒烟测试（需先 docker compose up -d mysql postgres）
python scripts/_smoke_multidb.py

# BIRD schema 向量化
python scripts/ingest_bird.py
```

## 已知瓶颈

1. **Self-Correction 修复率 7-20%** — Refiner 需重新设计
2. **SemCheck FN 率 50-65%** — 缺 gold 参照，方向：引入参照对比 + 硬规则层
3. **Guard FN 率 49-68%** — 纯形式校验无语义能力，方向：关键词→语法要求映射
4. **Generator 时间占比 50%+** — 多候选时翻倍

## Redis 连接策略

环境变量 `REDIS_URL` 优先 → 探测 `127.0.0.1:6379` → `redis:6379` → 全部失败返回 None（静默降级 no-op）。

## 编码约定

- SQLAlchemy 2.0：`with engine.connect() as conn:`
- 文件读写显式 `encoding='utf-8'`
- Prompt 从 `src/prompts.py` 引用，不硬编码
- Schema 统一走 `_get_cached_schema_info()`
- Pool/Executor 超时后 `shutdown(wait=False)`
- TraceLogger：`tlog.node_enter/exit`，LLM：`tlog.llm_call/llm_error`

## Commit 规则

- 每次 commit message 以 `@ vX.Y.Z:` 开头，标注版本号
- 版本号格式：`v大版本.小版本.补丁`
  - 新 feature/工具 → 小版本号 +1（如 v0.2.3 → v0.2.4）
  - bug fix/重构/优化 → 补丁号 +1（如 v0.2.3 → v0.2.4）
  - 架构重大变更 → 大版本号 +1
- Commit 后立即打对应 git tag：`git tag -a vX.Y.Z -m "@ vX.Y.Z: <描述>" && git push origin vX.Y.Z`
