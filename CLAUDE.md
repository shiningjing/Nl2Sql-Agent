# CLAUDE.md — NL2SQL Agent v0.5.5

## 项目概览

自然语言 → SQL 端到端系统。LangGraph 状态机编排（Router → Schema Retriever → Decomposer → Generator → Guard → Voter → SemCheck → Refiner）。BIRD Mini-Dev（500 题，11 DB，3 方言）消融评测。

**BIRD 全量 500 题结果** (2026-06-24)：

| 配置 | EX | 说明 |
|------|-----|------|
| R4_PruneFewshot | **37.6%** | 纯 RAG 最高配置（无 evidence），0 crash |
| R5_EvidenceFeedback | **42.4%** | R4 跑完后对 EX=0 题用 BIRD evidence 做 user_feedback 修复（24/311 修复，+4.8pp） |
| R2_RAG 100 题参考 | 39.0% | 100 题抽测 |
| R5_Evidence 100 题参考 | 43.0% | evidence 直接注入 prompt |
| Claude Opus 4.7 | 47.0% | 100 题抽测 |

RAG Table Recall **97.7%**（500 题）。500 次调用 **0 crash**，成本 **$2.47**。

**核心发现**：RAG 最大杠杆 (+11pp)；Decomposer 对 DeepSeek 无效；换强模型 +8pp；Self-Correction 修复率 24%；SemCheck 是最大优化空间（FN 39.7%）；Evidence 以 post-hoc feedback 方式使用可修复 7.7% 错题。

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
| W2  | MCP 工具 + SQL 安全    | ✅ 2 个 MCP 工具（validate_sql + execute_readonly_sql）、SQL 安全层（9 规则）、统一错误分类、Voter 按需激活     |
| W3  | 异步任务 + SSE + Human-Feedback | ✅ Kafka 异步 (T1)、Redis 心跳/TTL (T2)、SSE 流式 token (T3)、重试/取消 (T4)、Human-Feedback 多轮对话 |
| W4  | 部署 + Go 工具 + UI + 压测 + 文档 | Docker Compose、Go MCP Server、Streamlit ChatGPT 风格 UI、Go API 网关、三类压测、简历 |

### 优先级

- **P0**：模块化重构、自动评测、OTel tracing、MCP 工具层、SQL 安全、Kafka 异步、Redis 状态、SSE、Docker Compose、Go MCP Server、Go API 网关、README
- **P1**：Dead-letter queue、prompt 版本管理、失败回放、Grafana
- **P2**：gRPC、多租户、自动扩缩容、混合检索、成本治理

## W2 执行计划：MCP 工具 + SQL 安全层 + 统一错误分类 + Voter 优化 ✅ 完成 (v0.2.3–v0.2.7)

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

## W3 执行计划：异步任务 + SSE + Human-Feedback ✅ 完成 (v0.3.0–v0.3.7)

### 任务 1：Kafka 消息队列集成 ✅ (v0.3.0)

引入 Kafka 解耦 FastAPI 和 LangGraph 执行，支持异步任务提交。Worker 独立进程消费消息并跑 Graph，FastAPI 写入消息后立即返回 task_id。

**新增文件**：
- `infrastructure/broker.py` — `MessageBroker` 抽象 + `KafkaBroker`（kafka-python，KRaft 无 ZK）
- `infrastructure/task_store.py` — Redis 任务状态机（PENDING→RUNNING→SUCCESS/FAILED/TIMEOUT/CANCELLED）
- `worker/main.py` — Worker 独立进程（`python -m worker.main`）
- `api/routes/task.py` — 5 个端点（submit / status / cancel / stream / feedback）

**端点**：
| 端点 | 方法 | 作用 |
|------|------|------|
| `/task/submit` | POST | 提交任务 → Kafka → 返回 task_id（202） |
| `/task/{id}/status` | GET | 从 Redis 读取任务状态 |
| `/task/{id}/cancel` | POST | 请求取消，Worker 在节点间检查 |
| `/task/{id}/stream` | GET | SSE 流式推送进度 + SQL token |
| `/task/{id}/feedback` | POST | 人工反馈修正，最多 10 轮 |

**5 个 Topic**：
| Topic | 生产者 | 消费者 | 作用 |
|-------|--------|--------|------|
| `nl2sql.task.request` | FastAPI | Worker | 任务提交 |
| `nl2sql.task.status` | Worker | SSE/轮询 | 节点进度 |
| `nl2sql.task.result` | Worker | SSE/轮询 | 最终结果 |
| `nl2sql.task.feedback` | FastAPI | Worker | 人工修正指导 |
| `nl2sql.task.dlq` | Worker | 人工 | 死信（重试耗尽） |

**状态机**：PENDING → RUNNING → SUCCESS / FAILED / TIMEOUT / CANCELLED；feedback_transition() 允许 SUCCESS/FAILED → RUNNING

### 任务 2：Redis 任务状态机增强 ✅ (v0.3.1)

- `task_heartbeat()` / `task_get_heartbeat()` — 心跳保活
- `scan_stale_tasks()` — 基于 HEARTBEAT_STALE_S 扫描僵尸任务
- Worker `_heartbeat_loop()` — 独立线程持续心跳，即使 Graph/SQL 阻塞

### 任务 3：SSE 流式 SQL token 推送 ✅ (v0.3.2)

- `task_publish_token()` — Redis Pub/Sub 推送每个 SQL token
- `event_generator()` / `_listen()` — SSE 端点监听 Redis Pub/Sub + Kafka status
- `set_token_callback()` — Generator 注入回调，每生成一个 token 即推送

### 任务 4：重试/超时/取消 完善 ✅ (v0.3.3)

- Worker 取消检查：`task_is_cancelled()` 在节点间检查
- 协作式取消：被取消任务跳过收尾节点直接 CANCELLED
- 超时按 retry_count 递增（30s/45s/60s）
- DLQ 创建但不自动重放（用户决定：人工排查后手动重试）

### 任务 5：Human-Feedback 多轮对话 ✅ (v0.3.4–v0.3.7)

用户可在 Agent 返回结果后提供自然语言修正指导，Agent 从 Refiner 开始修正 SQL。最多 10 轮，完整对话记忆持久化 Redis。

**新增/修改文件**：
- `agent/state.py` — 新增 `user_feedback`、`conversation_turns`、`is_feedback_round` 字段
- `agent/graphs/feedback_graph.py` — 轻量修正图（Refiner→Generator→Guard→Voter→SemCheck）
- `agent/nodes/refiner.py` — `_format_user_feedback()` 格式化多轮对话历史
- `infrastructure/task_store.py` — `feedback_transition()` + 上下文持久化
- `api/routes/task.py` — `POST /task/{id}/feedback` 端点
- `worker/main.py` — `handle_feedback()` + `run_feedback_graph()`
- `ui/app.py` — 多轮对话 UI（折叠历史轮次 + feedback 输入框）
- `tests/test_feedback.py` — 16 个测试

### 任务 6：集成测试 + BIRD 冒烟 ✅ (v0.3.7)

- **全量测试**：188 passed（5 个 rate limit 429 已修复：`_RATE_LIMIT_MAX` 10→100）
- **集成测试**：66 passed（state machine、heartbeat、stale scan、timeout、TTL、token streaming、feedback graph）
- **BIRD 100 题随机 benchmark**（deepseek-v4-pro，R2_RAG + R5_Evidence）：

| 指标 | R2_RAG | R5_Evidence |
|------|--------|-------------|
| EX | **39.0%** | **43.0%** |
| VES | 0.49 | 0.43 |
| Crashed | 0/100 | 0/100 |
| Avg Time | 11.6s | 10.9s |
| Avg Tokens | 16,762 | 15,358 |
| RAG Recall | 98.4% | 97.9% |
| Self-Correction Fix | 26.5% | 24.1% |
| Cost | | $0.97 (¥7) |

- **结论**：R2_RAG EX 39.0% 持平基线 38.8%，0 crash，管道稳定，无回归

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
| 部署        | Docker Compose                       | + Go API 网关 (go-chi)        |

## W4 执行计划：部署 + Go 工具 + 压测 + 文档

### 任务 1：Docker Compose 一键启动

**目标**：`docker compose -f deployment/docker-compose.yml up -d` 启动全部服务。

**现状问题**：
- Dockerfile 只跑 Streamlit（`CMD streamlit run app.py`），缺少 API 和 Worker
- docker-compose.yml app 服务没有暴露 API 8000 端口
- 缺少 `.env.example`

**子任务**：
- 1.1 重写 Dockerfile — supervisord 或 shell 脚本启动 3 进程（API :8000 + Worker + Streamlit :8501）
- 1.2 补全 docker-compose — 暴露端口 8000/8501/8080，挂载 `llm_keys.json`、BIRD 数据、databases.json
- 1.3 `.env.example` 模板
- 1.4 验证：`docker compose -f deployment/docker-compose.yml up -d` → `curl localhost:8000/docs`

### 任务 2：Go MCP Server（P1 → W4）

**目标**：用 Go 重写 validate_sql 和 execute_readonly_sql，替换 Python MCP 工具。Python MCP Client 无需改动。

**技术栈**：
- `mark3labs/mcp-go` — MCP Server SDK
- `vitess/sqlparser` — SQL AST 解析（替代 sqlglot）
- `database/sql` — 数据库连接池

**子任务**：
- 2.1 初始化 `mcp-server-go/` Go module
- 2.2 `validate_sql` 工具 — AST 校验（语法/多语句/禁止类型），输出 `{valid, issues[], table_references}`
- 2.3 `execute_readonly_sql` 工具 — 只读 SQL 执行（双重校验 + LIMIT 自动包装 + 超时 + 行数上限）
- 2.4 安全硬限制：正则 + vitess AST 双重校验、连接级超时、max_rows 硬上限 1000
- 2.5 测试：合法 SELECT / 多语句拒绝 / DROP 拒绝 / 语法错误 / LIMIT 包装
- 2.6 Docker Compose 中加入 mcp-server-go 服务

### 任务 3：Streamlit UI 优化 — ChatGPT 风格对话

**目标**：改成 ChatGPT/Claude 风格的对话 UI，每轮独立气泡，中间过程可收起。

**具体改动**：

| # | 需求 | 说明 |
|---|------|------|
| 1 | 每轮独立气泡 | 所有用户发言和 Agent 回复都用独立 `st.chat_message` 渲染，交替排列，像 ChatGPT 那样 |
| 2 | 中间过程可收起 | 每个 Agent 回复顶部显示 Pipeline 节点耗时，默认折叠在 `st.expander` 里 |
| 3 | [＋新对话] 按钮 | 页面左上角，点击清空当前对话、开始新会话 |
| 4 | 标题缩小 | "NL2SQL Agent — BIRD" 改成小字副标题 |
| 5 | 单个输入框 | 自动判断：有活跃对话且最后结果成功 → 反馈模式；否则 → 新问题模式 |
| 6 | 删除对话 | 侧边栏每条历史右侧 🗑 按钮，删除单条 |
| 7 | 导出记录 | 侧边栏 Export 按钮，下载 JSON（全部历史）或 Markdown（当前对话） |

**对话结构**：

```
┌─ [＋新对话] ── NL2SQL Agent ──────────────────────────┐
│                                                        │
│  ┌─ User ────────────────────────────────────────┐     │
│  │ 查询每个客户的订单数                            │     │
│  └──────────────────────────────────────────────┘     │
│  ┌─ Assistant ───────────────────────────────────┐     │
│  │ ▸ Pipeline: Router 0.4s → Generator 5.2s → ..│     │
│  │   (点击展开中间过程详情)                        │     │
│  │                                               │     │
│  │ SQL: SELECT COUNT(*) ...                      │     │
│  │ Results: 1 row [table]                        │     │
│  └──────────────────────────────────────────────┘     │
│  ┌─ User ────────────────────────────────────────┐     │
│  │ 不要count，列出学校名字和数学成绩               │     │
│  └──────────────────────────────────────────────┘     │
│  ┌─ Assistant ───────────────────────────────────┐     │
│  │ ▸ Pipeline: Refiner → Generator 3.1s → ...    │     │
│  │ SQL: SELECT s.School, sc.AvgScrMath ...       │     │
│  │ Results: 12 rows [table]                      │     │
│  └──────────────────────────────────────────────┘     │
│  ┌──────────────────────────────────────────────┐     │
│  │ Ask a question or provide feedback...         │     │
│  └──────────────────────────────────────────────┘     │
│                                                        │
│  [侧边栏] Database / Samples / History / Export       │
└──────────────────────────────────────────────────────┘
```

### 任务 4：Go API 网关（P1 → W4）

**目标**：用 Go 写轻量 API 网关，统一入口，提供限流/健康检查/请求日志。

**技术栈**：
- `go-chi/chi` — HTTP 路由
- `httputil.ReverseProxy` — 反向代理到 Python FastaPI

**子任务**：
- 4.1 初始化 `gateway/` Go module
- 4.2 反向代理 — 所有请求转发到 FastaPI :8000
- 4.3 限流中间件 — 内存滑动窗口（100 req/min/IP），Redis 降级
- 4.4 聚合健康检查 — `GET /health` 返回 `{api, worker, redis, kafka, mcp}`
- 4.5 结构化请求日志 — method/path/status/elapsed/client_ip
- 4.6 Docker Compose 中加入 gateway 服务（对外暴露 8080）

### 任务 5：三类压测

**目标**：在 Docker 环境下验证功能/性能/稳定性。

**子任务**：
- 5.1 功能压测 — Docker 环境下 BIRD 20 题冒烟，确认 EX 无退化
- 5.2 性能压测 — 并发提交脚本（10/50/100 并发），测 P50/P95/P99 延迟、吞吐量
- 5.3 稳定性压测 — 长时间运行（30 分钟持续请求），检查内存/CPU 无泄漏，Kafka/Redis 无堆积
- 5.4 输出压测报告 `reports/stress_test.md`

### 任务 6：文档 + 简历材料

**子任务**：
- 6.1 README.md 重写 — 架构图（ASCII art）、快速开始（Docker Compose 三步）、API 概览、评测结果
- 6.2 简历项目描述 — 技术栈、成果数据（EX 39%、RAG 98.4%、200 次 0 crash）、Go MCP/网关亮点

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
    agent/                 # LangGraph 节点 (router, schema_retriever, decomposer, generator, guard, voter, executor, semantic_check, refiner)
      state.py             # AgentState TypedDict
      graphs/full_graph.py # 主图（条件边+循环）
    evaluation/            # BIRD 评测
    api/                   # FastAPI (routes: query, eval, task, health)
    worker/                # Kafka 消费者（独立进程，python -m worker.main）
    infrastructure/        # broker.py (Kafka 抽象) + task_store.py (Redis 状态机)
    storage/               # Redis 缓存 + config + db_registry
    guard/                 # safety_rules + error_types + error_classifier
    tools/                 # sql_executor
    observability/         # TraceLogger
    retrieval/             # RAG 管线
  corpus/bird_fewshot/     # Few-shot 示例（按 db_id + 方言）
  scripts/
    ingest_bird.py         # BIRD schema 向量化
  deployment/              # Dockerfile + docker-compose.yml (app + redis + pg + mysql + kafka)
  tests/                   # 188 tests
  reports/.gold_cache/     # Gold SQL 预计算结果
  logs/traces/             # 流式 trace (jsonl)
  data/bird/mini_dev_data/ # BIRD Mini-Dev 数据集
  llm_keys.json            # LLM 密钥（不提交 Git）
  databases.json           # 用户自定义数据库连接
  gateway/                 # Go API Gateway (go-chi 限流/反向代理)
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
# 评测（快速测试 / 全量 500 题 / 完整消融 / 预计算 gold）
python -m evaluation.run --test --samples 20 --configs R4
python -m evaluation.run --test --samples 500 --configs R5 --max-workers 8  # 全量 + Evidence Feedback
python -m evaluation.run --exp ablation --max-workers 8
python -m evaluation.precompute_gold

# 冒烟测试（需先 docker compose -f deployment/docker-compose.yml up -d mysql postgres）
python tests/smoke_multidb.py

# BIRD schema 向量化
python scripts/ingest_bird.py
```

## 已知瓶颈 (2026-06-24 全量 500 题 R4 配置)

1. **SemCheck FN 率 39.7%** — 186/468 题 LLM 判 YES 但 EX=0，最大优化空间（修复可带来 +4-5pp）
2. **Self-Correction 修复率 24.4%** — 266 题重试只修了 65 题，Refiner 错误格式化质量是瓶颈
3. **Guard FN 率 58.5%** — 纯形式校验无语义能力，468 通过中 281 实际错误
4. **Decomposer 复杂题 EX 27.5%** vs 简单题 44.4% — 17pp 差距，对 DeepSeek 拆解可能反效果
5. **Voter 候选多样性不足** — 多候选去重后 145/266 只剩 1 个，不同 temp 产出高度相似
6. **Evidence Feedback 修复率 7.7%** — post-hoc evidence 作为 user_feedback 效果有限，24/311 修复 → EX 42.4%

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
