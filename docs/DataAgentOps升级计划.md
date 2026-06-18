# DataAgentOps 升级项目四周实施计划

## 1. 项目背景

现有 NL2SQL Agent 已具备较完整的核心功能，包括 LangGraph 工作流编排、Schema 与领域知识 RAG、列级剪枝、SQL Guard、多候选投票、语义校验以及执行错误驱动的自动修复。BIRD 消融结果：DeepSeek V4 Pro R5_Evidence **38.8% EX**（日常使用），Claude Opus 4.7 R5_Evidence 47.0% EX（最强模型）。

当前项目的主要不足不在 SQL 生成算法本身，而在生产级 Agent 工程能力，包括工具标准化接入、全链路可观测性、异步任务处理、流式响应、自动评测、安全控制和容器化部署。

本次升级目标是将现有 NL2SQL Agent 从单体应用提升为一个可观测、可评测、可扩展、可部署的 Data Agent 平台。

---

## 2. 项目目标

四周内完成以下五项核心能力：

1. 建立统一的 **MCP 工具层**，对 Schema 检索、SQL 校验、SQL 执行和查询解释等能力进行标准化封装
2. 建立 **AgentOps 可观测体系**，支持调用链追踪、节点耗时、Token 消耗、错误归因和失败回放
3. 建立基于 **Kafka + Redis** 的异步任务处理链路，支持任务状态管理、失败重试和幂等控制
4. 支持 **SSE 流式返回** Agent 执行过程和中间状态
5. 完成 **Docker Compose 与 Kubernetes 部署**，形成可演示、可测试的完整工程

> 本次升级不以提高模型精度为首要目标，而以补齐 Agent 平台工程能力为主。升级后 BIRD EX 不应低于当前 DeepSeek 基线（38.8%）。

---

## 3. 目标系统架构

### 3.1 业务链路

```
Web UI / API Client
        │
        ▼
FastAPI Gateway ── SSE 流式响应
        │
        ▼
Kafka Task Queue
        │
        ▼
LangGraph Agent Worker
        │
        ├── Schema Retrieval
        ├── SQL Generation
        ├── Guard
        ├── Voter
        ├── Semantic Check
        └── Error Repair
        │
        ▼
MCP Tool Layer
        │
        ├── Schema Service
        ├── SQL Validator
        ├── SQL Explain
        ├── Read-only Executor
        └── Domain Knowledge Retriever
        │
        ▼
PostgreSQL / ChromaDB / Redis
```

### 3.2 可观测链路

```
FastAPI + LangGraph + MCP Tools
              │
              ▼
       OpenTelemetry
              │
              ▼
Tracing / Metrics / Logs
```

### 3.3 组件职责

| 组件            | 职责                      |
| ------------- | ----------------------- |
| FastAPI       | 请求入口、任务提交、任务查询、SSE 返回   |
| Kafka         | 解耦请求入口与 Agent 执行，异步任务队列 |
| Redis         | 任务状态、执行进度、临时结果、幂等标识     |
| LangGraph     | 执行 NL2SQL 多阶段状态机        |
| MCP           | 统一注册数据库与知识检索工具          |
| PostgreSQL    | 业务数据库和测试数据库             |
| ChromaDB      | Schema 与领域知识向量检索        |
| OpenTelemetry | 采集 trace、metric 和 error |
| Docker/K8s    | 容器化与部署                  |

---

## 4. 四周开发安排

### 第 1 周：重构现有项目并建立 AgentOps 基础

**目标**：将现有 Agent 重构为模块化工程，建立自动评测和全链路追踪基础。

**总用时**：5 天。

---

#### 任务 1：代码结构重构（1.5 天）

**现状**：

- `nl2sql/` — 8 个文件混杂了检索、执行、工具、配置
- `src/` — agent、api、eval、retrieval、obs、guardrails，检索逻辑与 `nl2sql/rag_retrieve.py` 分散
- `nl2sql/schema.py` 和 `src/retrieval/` 存在两条 schema 获取路径

**目标结构**：

```
data-agent-ops/
├── api/              ← src/api/ 迁移
├── agent/            ← src/agent/ + nl2sql/generate.py
│   ├── state.py
│   ├── graphs/
│   └── nodes/        (9 个节点)
├── retrieval/        ← 合并 src/retrieval/ + nl2sql/rag_retrieve.py + nl2sql/schema.py
├── tools/            ← 新建：nl2sql/execute.py 迁入，预留 MCP client
├── guard/            ← src/guardrails/ + nl2sql/pipeline.py 的错误分类部分
├── evaluation/       ← src/eval/
├── observability/    ← src/obs/ 升级为 OpenTelemetry
├── storage/          ← 新建：nl2sql/db_registry.py + src/infrastructure/redis_cache.py
├── deployment/       ← 新建：Dockerfile, docker-compose 占位
└── tests/            ← 新建：分散在 scripts/ 的测试收敛
```

**子任务**：

| #    | 子任务                 | 内容                                                                                    | 风险                    |
| ---- | ------------------- | ------------------------------------------------------------------------------------- | --------------------- |
| 1.1  | 目录创建                | 建 10 个顶层目录，每个放 `__init__.py`                                                          | 低                     |
| 1.2  | `agent/` 迁移         | `src/agent/*` → `agent/`，`nl2sql/generate.py` → `agent/generator_llm.py`              | 低                     |
| 1.3  | `api/` 迁移           | `src/api/*` → `api/`，路径重新布线                                                           | 中 — import 路径全改       |
| 1.4  | `retrieval/` 合并     | `src/retrieval/*` + `nl2sql/rag_retrieve.py` + `nl2sql/schema.py` → `retrieval/`      | 中 — 消除 schema 获取的两条路径 |
| 1.5  | `tools/` 新建         | `nl2sql/execute.py` → `tools/sql_executor.py`，预留 `tools/mcp_client.py`                | 低                     |
| 1.6  | `guard/` 迁移         | `src/guardrails/` → `guard/`，`nl2sql/pipeline.py` 错误分类 → `guard/error_classifier.py`  | 低                     |
| 1.7  | `evaluation/` 迁移    | `src/eval/*` → `evaluation/`                                                          | 低                     |
| 1.8  | `observability/` 新建 | `src/obs/` → `observability/`，保留老 TraceLogger 做过渡                                     | 低                     |
| 1.9  | `storage/` 新建       | `nl2sql/db_registry.py` → `storage/`，`src/infrastructure/redis_cache.py` → `storage/` | 低                     |
| 1.10 | 全局 import 重布线       | 搜所有 `from nl2sql.` `from src.` 替换为新路径                                                 | 高 — 量大、遗漏风险           |
| 1.11 | 冒烟验证                | `_smoke_multidb.py` 跑通 + Streamlit UI 正常启动 + BIRD 单题跑通                                | —                     |

---

#### 任务 2：统一 Agent State（0.5 天）

**当前状态**：`src/agent/state.py` 已有 AgentState TypedDict，但字段不全，缺少 token_usage、node_latency 等可观测字段。

**目标 State**：

```python
class AgentState(TypedDict):
    # 请求标识
    request_id: str
    trace_id: str                      # ← 新增：OTel trace ID
    # 输入
    user_query: str
    db_id: str                         # ← 新增：数据库标识
    # Schema & 检索
    retrieved_schema: str
    selected_columns: list[str]
    domain_knowledge: str              # ← 新增：领域知识
    # SQL 生成
    decomposed_questions: list[str]
    generated_sql: str
    candidate_sqls: list[dict]         # [{sql, reason, score}]
    # 校验 & 执行
    guard_result: dict
    voter_result: dict
    semcheck_result: dict
    execution_result: dict
    executor_error: str | None         # ← 新增：执行错误详情
    # 修复
    repair_count: int
    max_repair: int                    # ← 新增：最大修复次数
    error_type: str
    repair_history: list[dict]         # ← 新增：每轮修复记录
    # 可观测
    token_usage: dict                  # {node_name: {prompt, completion, total}}
    node_latency: dict                 # {node_name: ms}
    # 输出
    final_sql: str
    final_answer: list[dict]           # 执行结果行
    success: bool
```

**子任务**：

| #   | 子任务                                                           |
| --- | ------------------------------------------------------------- |
| 2.1 | 定义新增字段，写 TypedDict                                            |
| 2.2 | 在 9 个 LangGraph 节点中接入新字段（每个节点写入自身 token_usage 和 node_latency） |
| 2.3 | `full_graph.py` 确保新字段在节点间传递                                   |

---

#### 任务 3：可观测性增强 — TraceLogger + OTel 导出（1 天）

**原则**：TraceLogger 已经覆盖了节点 enter/exit、LLM 调用、token 统计、错误分类。不替代，只在现有基础上补齐缺口 + 加 OTel 导出薄层。

**现状 vs 计划要求**：

| 计划要求的 Span Attribute         | TraceLogger 现状                 | 做法               |
| ---------------------------- | ------------------------------ | ---------------- |
| 输入摘要                         | `node_enter` 的 `meta` 参数       | 已有               |
| 执行时间                         | `node_exit` 的 `duration_s`     | 已有               |
| 错误类型                         | `node_error` 的 `error_type`    | 已有               |
| 输出状态 (success/error/skipped) | node_exit 和 node_error 是两个独立事件 | **补 status 字段**  |
| Token 用量                     | `llm_call` 事件有，但没挂在 node 下     | **补 node 归属**    |
| 模型                           | `llm_call` 的 `model`           | **补到 node_exit** |
| prompt 版本                    | 完全没有                           | **补字段**          |

**需要补的三个东西**：

```python
# ① node_exit 加 status / model / prompt_version
tlog.node_exit("generator", {
    "sql_len": 150,
    "status": "success",          # ← 新增
    "model": "deepseek-v4-pro",   # ← 新增
    "prompt_version": "v2",       # ← 新增
})

# ② node_enter 加 model / prompt_version
tlog.node_enter("generator", {
    "question_len": 42,
    "model": "deepseek-v4-pro",   # ← 新增
    "prompt_version": "v2",       # ← 新增
})

# ③ llm_call 加 node 归属（知道这个 token 花在哪个节点）
tlog.llm_call("deepseek-chat", usage, node="generator")  # ← 新增参数
```

**OTel 导出薄层**：在 TraceLogger 的 `_emit()` 里加一个可选出口，将增强后的事件翻译成 OTel span 发到 Jaeger：

```python
# observability/otel_bridge.py —— 薄薄一层，不替代 TraceLogger
class OtelBridge:
    def on_event(self, event: dict):
        # node_enter → 创建 span
        # node_exit  → 关闭 span，填入 status/model/token/duration
        # 其他事件  → span.add_event()
        ...
```

**子任务**：

| #   | 子任务                                                                              |
| --- | -------------------------------------------------------------------------------- |
| 3.1 | TraceLogger 补 3 个字段：`status`, `model`, `prompt_version` + `llm_call` 加 `node` 参数 |
| 3.2 | 写 `observability/otel_bridge.py`：监听 TraceLogger 事件 → 翻译为 OTel span               |
| 3.3 | 安装 OTel 依赖 + Jaeger 容器，验证瀑布图能展示完整节点链路                                            |
| 3.4 | FastAPI 中间件注入 `trace_id` → response header（已有机房，补字段即可）                           |

**LLM 调用的时间和 token 用量也通过 OTel Bridge 绑定到当前 Span**

> 使用前需要确认 LLM Token 用量是否可以通过 ChatOpenAI/ChatAnthropic 的响应数据提取，以便在 Span 中记录 prompt_tokens 和 completion_tokens。

**选型**：`opentelemetry-api` + `opentelemetry-sdk` + `opentelemetry-exporter-otlp`，本地用 Jaeger 做 UI。

---

#### 任务 4：自动评测脚本（1 天）

**现状**：`scripts/eval_bird.py` 已有 `--test` 和 `--exp ablation` 模式，`src/eval/metrics.py` 有 `exec_match` 和 `VES`。缺的是**统一入口 + 完整输出 + 报告格式化**。

**目标命令**：

```bash
python -m evaluation.run --config R5 --samples 100 --output reports/
```

**输出指标**：

| 指标                         | 当前是否已有       |
| -------------------------- | ------------ |
| EX accuracy                | ✅ exec_match |
| VES                        | ✅ VES        |
| SQL execution success rate | ❌            |
| First-pass success rate    | ❌            |
| Repair success rate        | ❌            |
| Average repair count       | ❌            |
| Average latency            | ❌            |
| P95 latency                | ❌            |
| Average token consumption  | ❌            |

**子任务**：

| #   | 子任务                                                                                                                  |
| --- | -------------------------------------------------------------------------------------------------------------------- |
| 4.1 | 写 `evaluation/reporter.py` — 统一收集每条评测的详细结果：SQL、执行时间、token、错误类型、修复次数、成功/失败                                            |
| 4.2 | 扩展 `evaluation/metrics.py` — 补充 execution_success_rate、first_pass_rate、repair_success_rate、avg/P95 latency、avg token |
| 4.3 | 改造 `evaluation/run.py`（新文件）为统一入口，支持 `--config`, `--samples`, `--output`, `--format json                              |
| 4.4 | JSON 报告格式设计 + CSV 导出                                                                                                 |
| 4.5 | 跑一次 BIRD 500 题全量 → 生成基线报告 `reports/baseline_W1.json`                                                                 |
| 4.6 | 保留老 `scripts/eval_bird.py` 兼容调用，加上 deprecation warning                                                               |

---

#### 任务 5：回归验证（0.5 天）

> W1 最关键的卡点：重构后 BIRD EX 不能低于当前 DeepSeek 基线（38.8%）。

| #   | 子任务      | 验证项                                           |
| --- | -------- | --------------------------------------------- |
| 5.1 | 多数据库冒烟   | `_smoke_multidb.py` 9 题 × 3 方言全部通过            |
| 5.2 | UI 全链路   | Streamlit 提交查询 → 返回 SQL → 执行 → 展示结果           |
| 5.3 | API 接口   | FastAPI `/api/v1/query` 正常返回                  |
| 5.4 | BIRD 抽测  | 100 题抽测 EX ≥ 36%（允许 2pp 波动，DeepSeek 基线 38.8%） |
| 5.5 | Trace 对比 | 对比重构前后的 trace 日志，确认节点输出一致                     |

---

#### 时间依赖

```
任务 1 (代码重构) ──┬──▶ 任务 2 (State 定义)
  1.5 天            │      0.5 天
                    │
                    ├──▶ 任务 3 (TraceLogger增强+OTel)
                    │      1 天
                    │
                    └──▶ 任务 4 (评测脚本)
                           1 天
                              │
                              ▼
                    任务 5 (回归验证)
                           0.5 天

**总用时**：4.5 天
```

---

#### 验收标准

- [ ] 原有 Agent 功能在重构后正常运行
- [ ] BIRD EX 不低于当前 DeepSeek 基线（38.8%）
- [ ] 每个核心 LangGraph 节点均可在 trace 中查看
- [ ] 单条请求能完整展示节点执行时间和错误信息
- [ ] 自动评测脚本一键运行并生成报告

#### 交付物

- 模块化代码结构
- LangGraph State 定义
- OpenTelemetry 基础接入
- BIRD 自动评测脚本
- 第一版性能基线报告

---

### 第 2 周：MCP 工具化与安全控制

**目标**：将数据库访问和知识检索能力封装为标准化 MCP 工具，建立安全执行边界。

#### 任务清单

1. **实现 MCP Server**（Go 实现，通过 MCP 协议与 Python Agent 解耦）
   
   - `get_database_schema` — Schema 反射
   - `search_relevant_tables` — 向量检索相关表
   - `search_relevant_columns` — 列级检索
   - `retrieve_domain_knowledge` — 领域知识检索
   - `validate_sql` — AST 语法校验
   - `explain_sql` — 查询计划解释
   - `execute_readonly_sql` — 只读执行沙箱
   - 每个工具定义：输入参数、输出 Schema、超时、错误类型、权限范围

2. **LangGraph 节点改造为 MCP 调用**
   
   - 将直接访问数据库/检索模块的代码替换为 MCP Client 调用

3. **SQL 安全层**
   
   - 只允许 SELECT，禁止 INSERT/UPDATE/DELETE/DROP/ALTER
   - 限制可访问 Schema、表、列
   - 自动增加查询行数限制
   - 设置查询超时
   - 拦截多语句 SQL
   - 敏感字段脱敏
   - 工具调用权限校验

4. **统一错误分类**
   
   - RETRIEVAL_ERROR, INVALID_SCHEMA, INVALID_COLUMN, SQL_SYNTAX_ERROR
   - SQL_EXECUTION_ERROR, PERMISSION_DENIED, TIMEOUT
   - EMPTY_RESULT, SEMANTIC_MISMATCH, TOOL_UNAVAILABLE

#### 验收标准

- [ ] LangGraph 通过 MCP 完成完整 NL2SQL 流程
- [ ] MCP 工具可独立测试和调用
- [ ] 危险 SQL 被稳定拦截
- [ ] 所有 MCP 调用进入 OpenTelemetry trace
- [ ] 工具失败返回统一错误码

#### 交付物

- MCP Server (Go) + Client (Python)
- 七个核心 MCP 工具
- SQL 安全规则
- 统一错误类型定义
- MCP 工具测试集

---

### 第 3 周：异步任务、Kafka/Redis 与 SSE

**目标**：将同步 Agent 请求改造成异步任务执行模式，支持流式返回中间过程。

#### 任务清单

1. **任务状态机**
   
   ```
   PENDING → RUNNING → RETRIEVING → GENERATING → VALIDATING
                    → EXECUTING → REPAIRING → SUCCEEDED / FAILED
                    → CANCELLED
   ```

2. **Kafka 接入**
   
   - Topic: `agent-task`, `agent-retry`, `agent-dead-letter`, `agent-event`
   - 流程：API 生成 task_id → 写入 agent-task → Worker 消费执行 → 中间事件写入 agent-event → 失败进入 retry 或 dead-letter

3. **Redis 任务状态管理**
   
   - 保存：task_id, 当前状态, 当前节点, 执行进度, 中间 SQL, 错误信息, 最终结果, 创建/更新时间, 幂等键

4. **SSE 流式接口**
   
   ```
   POST /tasks              # 提交任务
   GET  /tasks/{task_id}    # 查询状态
   GET  /tasks/{task_id}/events  # SSE 事件流
   POST /tasks/{task_id}/cancel  # 取消任务
   ```
   
   - SSE 事件：task_started, schema_retrieved, sql_generated, sql_validated, sql_executed, repair_started, task_completed, task_failed

5. **可靠性机制**
   
   - 最大重试次数 + 指数退避
   - 请求幂等（idempotency key）
   - 任务超时
   - Worker 异常恢复
   - Dead-letter queue
   - 用户取消任务
   - 数据库连接失败降级

#### 验收标准

- [ ] API 提交后立即返回 task_id
- [ ] Agent 任务由 Kafka Worker 异步执行
- [ ] SSE 可看到任务中间状态
- [ ] 重复提交相同幂等键不重复执行
- [ ] Worker 中途退出后未完成任务可被重新消费
- [ ] 超时/多次失败任务进入 dead-letter queue

#### 交付物

- Kafka 异步执行链路
- Redis 任务状态管理
- SSE 流式接口
- 重试、超时、幂等和取消机制
- 异步任务演示

---

### 第 4 周：部署、压测、文档与简历包装

**目标**：完成容器化部署、系统测试、性能评估和项目材料整理。

#### 任务清单

1. **Docker Compose 一键部署**
   
   - 编排：FastAPI Gateway, Agent Worker, MCP Server, PostgreSQL, Redis, Kafka, ChromaDB, OTel Collector

2. **Kubernetes 部署**（P1）
   
   - Deployment, Service, ConfigMap, Secret
   - Readiness/Liveness probe
   - Resource request/limit
   - Worker 副本扩展
   - 基础水平扩缩容

3. **系统压测**
   
   - **功能测试**：单表查询、多表 Join、聚合、含业务知识、错误修复、越权拦截
   - **性能测试**：端到端/P95/P99 延迟、吞吐量、Kafka 排队时间、各节点耗时、Token 消耗、MCP 工具耗时
   - **稳定性测试**：Worker 重启、MCP 不可用、PG 超时、Kafka 消费失败、Redis 中断、重复请求、长查询取消

4. **项目文档（README）**
   
   - 项目背景、系统架构图、核心模块、快速启动
   - MCP 工具列表、API 文档、Agent 工作流图
   - Trace 示例、评测结果、性能测试
   - 故障恢复演示

5. **求职材料**
   
   - 一页系统架构图、LangGraph 流程图、OTel Trace 截图、性能指标表
   - 2 分钟项目介绍 + 5 分钟深度技术讲解
   - 简历项目描述 + 常见面试问题清单

#### 验收标准

- [ ] Docker Compose 完整启动系统
- [ ] K8s 环境可部署核心服务
- [ ] 所有请求具有 trace_id
- [ ] 异步任务成功率、P95 延迟和失败类型可统计
- [ ] 至少完成一次 Worker 故障恢复演示
- [ ] README 支持其他开发者独立启动项目

---

## 5. 项目最终验收指标

### 功能指标

- [ ] 完整 NL2SQL 生成、执行和修复流程
- [ ] MCP 工具化调用
- [ ] 异步任务和 SSE 状态返回
- [ ] 危险 SQL 拦截
- [ ] 自动评测和失败回放
- [ ] Docker 与 K8s 部署

### 可观测性指标

- [ ] 核心 Agent 节点 trace 覆盖率 100%
- [ ] 每次请求可查询节点耗时、Token 消耗和错误类型
- [ ] 支持按任务、节点、错误类型和模型版本检索执行记录

### 效果指标

- [ ] BIRD EX ≥ 38.8%（DeepSeek 基线）
- [ ] 单独统计首次生成正确率和修复后正确率
- [ ] 给出 SQL 修复成功率和平均修复次数
- [ ] 给出平均延迟和 P95 延迟
- [ ] 给出单请求平均 Token 消耗

### 工程指标

- [ ] 一键启动
- [ ] 任务幂等、重试、超时和取消
- [ ] Worker 异常后任务可恢复
- [ ] MCP 工具统一输入输出和错误协议
- [ ] 所有配置和密钥从代码中分离

---

## 6. 优先级控制

### P0（必须完成）

项目模块化重构、BIRD 自动评测、OpenTelemetry tracing、MCP 工具层、SQL 安全控制、Kafka 异步任务、Redis 状态管理、SSE 流式返回、Docker Compose、README 与简历材料

### P1（尽量完成）

Kubernetes 部署、Dead-letter queue、任务取消、Prompt/模型版本管理、失败案例 replay、Grafana 指标看板

### P2（时间充足再完成）

Go 工具网关、gRPC/Protobuf 内部通信、多租户权限隔离、自动弹性扩缩容、混合检索和 reranker、完整 Agent 成本治理

> 四周内不建议投入大量时间重写模型、替换向量数据库或重新设计 NL2SQL 算法。

---

## 7. Go 技术栈切入点

### 7.1 MCP Server（推荐 P0/P1 — 第 2 周）

用 Go 实现 MCP Server，Python Agent 通过 MCP Client 调用。优势：

- `database/sql` + 连接池是标准库一等公民
- SQL 解析库成熟（`vitess/sqlparser`、`pingcap/tidb-parser`）
- 高并发工具调用天然适合 goroutine
- MCP 协议 Go SDK（`mark3labs/mcp-go`）已稳定

架构：

```
LangGraph Agent (Python) ──MCP Client──▶ MCP Server (Go)
                                              │
                                              ├── Schema Service (连接池 + 缓存)
                                              ├── SQL Validator (AST 级校验)
                                              ├── SQL Executor (沙箱执行)
                                              └── Domain Knowledge (调 ChromaDB)
```

### 7.2 API 网关（P1/P2）

Go `go-chi` 或 `grpc-gateway` 做限流、鉴权、请求路由、协议转换（REST → gRPC → Kafka），渐进式替换 FastAPI 网关。

### 7.3 Schema 缓存服务（P2）

Go 常驻内存 Schema 缓存 + gRPC，毫秒级响应，多 Worker 一致性好于 Python `lru_cache`。

---

## 8. 最终简历表述

**DataAgentOps：可观测、可评测的企业级 NL2SQL Agent 平台**

方向：AI Agent / Data Agent / AI4DB

**项目简介**：面向多 Schema 业务数据库构建生产级 NL2SQL Agent 平台，在 LangGraph 状态机、分层 RAG、SQL 多候选投票和执行错误自修复基础上，引入 MCP 工具协议、Kafka 异步任务、全链路可观测性和自动评测体系。

**核心工作**：

- 使用 LangGraph 编排 Schema 检索、列级剪枝、SQL 生成、Guard 校验、多候选投票、执行验证和错误修复流程
- 将 Schema 查询、SQL 校验、Explain 和只读执行能力封装为 MCP 工具（Go 实现），实现数据库能力标准化接入
- 基于 Kafka 和 Redis 构建异步任务链路，支持幂等、超时、失败重试、任务取消和 SSE 流式状态返回
- 基于 OpenTelemetry 建立节点级 tracing 和评测体系，统计端到端成功率、P95 延迟、Token 消耗、修复成功率和错误分布
- 设计 AST 级 SQL 白名单、只读权限、敏感字段过滤和查询超时机制
- 完成 Docker Compose 与 Kubernetes 部署，支持 Agent Worker 横向扩展和故障恢复

**技术栈**：Python, LangGraph, LangChain, Go, MCP, Kafka, Redis, PostgreSQL, ChromaDB, FastAPI, OpenTelemetry, Docker, Kubernetes

**成果**：

- BIRD benchmark EX 38.8%（DeepSeek V4 Pro）/ 47.0%（Claude Opus 4.7），相比裸 LLM 基线（23.4%）分别提升 15 和 24 个百分点
- 异步任务成功率、SQL 修复成功率、P95 延迟和单请求成本：（升级后填写实测结果）
