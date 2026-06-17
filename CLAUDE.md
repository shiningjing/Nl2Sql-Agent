# CLAUDE.md — NL2SQL Agent v0.2.1 → DataAgentOps

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
| W2  | MCP 工具化 + 安全     | 7 个 MCP 工具、LangGraph 节点改为 MCP 调用、SQL 安全层（9 规则）、统一错误分类     |
| W3  | 异步任务 + SSE       | Kafka (4 Topic) + Redis 任务状态机、SSE 流式接口、重试/幂等/超时/取消        |
| W4  | 部署 + 压测 + 文档     | Docker Compose 一键启动、K8s 部署、三类压测（功能/性能/稳定性）、简历材料           |

### 优先级

- **P0**：模块化重构、自动评测、OTel tracing、MCP 工具层、SQL 安全、Kafka 异步、Redis 状态、SSE、Docker Compose、README
- **P1**：K8s、Dead-letter queue、任务取消、prompt 版本管理、失败回放、Grafana
- **P2**：Go 工具网关、gRPC、多租户、自动扩缩容、混合检索、成本治理

### Go 技术栈切入点

- **MCP Server 用 Go 写**（推荐 P0/P1）：`database/sql` 连接池管理、`vitess/sqlparser` AST 校验、`mark3labs/mcp-go` SDK，与 Python Agent 通过 MCP 协议解耦
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
3. Generator temperature=0；多候选 0/0.3/0.6 + 去重早停
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
