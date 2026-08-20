<p align="right"><a href="README.md">English</a></p>

# NL2SQL Agent

自然语言 → SQL 端到端系统。LangGraph 状态机编排 **Router → Schema Retriever → Decomposer → Generator → Guard → Voter → Semantic Check → Refiner**，集成 RAG 增强检索与 Self-Correction 自修复循环。

> **BIRD Mini-Dev**（100 题）：DeepSeek V4 Pro EX **39.0%** · RAG 表召回 **98.4%** · 200 次调用 **0 crash** · 成本 **$0.97**（¥7）

## 架构

### 系统

```
  Client (Streamlit UI / API)
        │
        │  POST /task/submit    POST /task/{id}/feedback
        │  GET  /task/{id}/stream (SSE)
        ▼
  Spring Gateway (:8080)
        │  异步任务路径（Java 原生，不经 FastAPI）：
        │    submit ──→ Redis 状态 + Kafka ──→ Worker（Python，零改动）
        │    SSE   ←── Redis PubSub(token) + 状态轮询 ◄──┘
        │
        │  同步查询路径（熔断+超时转发）：
        │    /query/full/stream ──→ FastAPI(:8000) 线程内跑 LangGraph
        ▼
  ┌──────────────────────────────────────────────────────┐
  │  PostgreSQL · MySQL · SQLite (BIRD 11 数据库)        │
  │  ChromaDB (RAG 向量库) · Redis (任务状态 + Token 流) │
  └──────────────────────────────────────────────────────┘
```

### LangGraph Agent

```
                         ┌─────────────────────┐
                         │       Router        │
                         │   简单 / 复杂 判定   │
                         └──────────┬──────────┘
                                    │
                         ┌──────────▼──────────┐
                         │   Schema Retriever  │
                         │   RAG + DDL 构建    │
                         └──────────┬──────────┘
                                    │
                         复杂 ──────┴────── 简单
                           │                │
                ┌──────────▼──────────┐     │
                │     Decomposer      │     │
                │   子问题 DAG 拆解    │     │
                └──────────┬──────────┘     │
                           │                │
                           └──────┬─────────┘
                                  │
                       ┌──────────▼──────────┐
                       │   Few-shot Selector │
                       │   Top-K 示例检索     │
                       └──────────┬──────────┘
                                  │
                       ┌──────────▼──────────┐
                       │      Generator      │
                       │  单候选 (temp=0)     │
                       │  重试时多候选        │
                       └──────────┬──────────┘
                                  │
                       ┌──────────▼──────────┐
                       │        Guard        │
                       │   AST + 9 规则      │
                       └──────────┬──────────┘
                                  │
                        通过 ─────┴───── 拒绝
                          │                │
               ┌──────────▼──────────┐  ┌──▼──────────┐
               │        Voter        │  │   Refiner   │◄── 人工反馈
               │  并行执行 +          │◄─│  错误 →     │   (POST /task/
               │  LLM 平票兜底       │  │  Generator  │    {id}/feedback)
               └──────────┬──────────┘  └──▲──────────┘
                          │                │
                 有优胜 ───┴── 无优胜 ─────┘
                          │
               ┌──────────▼──────────┐
               │   Semantic Check    │
               │   LLM YES / NO      │
               └──────────┬──────────┘
                          │
                  通过 ───┴─── 不通过 ──→ Refiner
                          │
                    ┌─────▼─────┐
                    │    END    │
                    └───────────┘
```

## 快速开始

### 1. 配置

```bash
cp .env.example .env
# 在 llm_keys.json 中填入密钥：
# {"deepseek": "sk-xxx", "openai": "sk-xxx", "anthropic": "sk-ant-xxx"}
```

### 2. 启动

```bash
docker compose -f deployment/docker-compose.yml up -d
```

一键启动 6 个服务：**FastAPI** (:8000) + **Streamlit** (:8501) + **Redis** (:6379) + **PostgreSQL** (:5432) + **MySQL** (:3306) + **Kafka** (:9092)。

如需 Go 服务：

```bash
cd tools/mcp-server-go && go build -o mcp-server-go.exe . && cd ../..
cd gateway && go build -o gateway.exe . && cd ..
```

### 3. 打开

```bash
open http://localhost:8501      # Streamlit 前端
open http://localhost:8000/docs  # FastAPI Swagger 文档
```

### 快速测试

```bash
# 多数据库冒烟测试
python tests/smoke_multidb.py

# BIRD 20 题快速验证
python -m evaluation.run --test --samples 20 --configs R2
```

## 功能特性

| 特性 | 说明 |
|------|------|
| **LangGraph 管线** | 9 节点状态机：Router → Schema → Decomposer → Fewshot → Generator → Guard → Voter → SemCheck → Refiner |
| **RAG 检索** | ChromaDB + BAAI/bge-small-zh-v1.5 向量化。BIRD 表召回率 **98.4%** |
| **SQL 安全 9 规则** | DDL 拦截 · 多语句检测 · LIMIT 兜底 · 查询超时 · 敏感列告警 · 表/列存在性校验 · WHERE 告警 · 子查询深度限制 · JOIN 表数限制 |
| **MCP 工具** | `validate_sql`（AST 校验）+ `execute_readonly_sql`（沙箱执行）。Python + Go 双实现 |
| **Self-Correction** | Guard/Voter/SemCheck 失败 → Refiner 格式化错误 → Generator 重试。修复率 **26.5%** |
| **Human Feedback** | 多轮自然语言修正（上限 10 轮），完整对话历史持久化 Redis |
| **SSE 流式** | Redis Pub/Sub 实时 token 推送 + Server-Sent Events |
| **异步任务** | Kafka 提交任务 → Worker 消费 → Redis 状态机（PENDING→RUNNING→SUCCESS/FAILED/TIMEOUT/CANCELLED） |
| **Spring 网关** | Java 21 / Spring Boot 3.3（虚拟线程）· /api/v1 透明代理 · SSE 流式透传 · Resilience4j 超时熔断 · traceId 贯穿 · Prometheus /metrics |
| **Go MCP Server** | vitess/sqlparser AST 校验 · database/sql 连接池 · 正则+AST 双重校验 · LIMIT 自动包装 · 语句超时 |
| **多数据库** | SQLite / MySQL / PostgreSQL。方言自动识别 + 方言专属 Few-shot |
| **多模型** | DeepSeek V4 Pro / GPT-4o / Claude Opus 4.7 / 自定义。自动识别 Anthropic → ChatAnthropic |
| **可观测性** | OpenTelemetry 全链路追踪 · TraceLogger (jsonl) · 节点级耗时 · Token 用量 |
| **Docker 部署** | 6 服务一键启动。KRaft Kafka（无需 ZooKeeper）。内置 Demo 数据库 |

## API 端点（16 个）

### 任务（异步）

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/task/submit` | 提交 NL2SQL 任务 → Kafka → 返回 task_id (202) |
| `GET` | `/task/{id}/status` | 从 Redis 查询任务状态 |
| `POST` | `/task/{id}/cancel` | 取消运行中的任务 |
| `GET` | `/task/{id}/stream` | SSE 流式推送（token + 节点进度 + 结果） |
| `POST` | `/task/{id}/feedback` | 提交人工反馈进行多轮修正 |
| `GET` | `/task/{id}/health` | 任务健康检查 + 心跳 |
| `POST` | `/task/scan-stale` | 扫描僵尸/过期任务 |

### 查询（同步）

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/query` | 简单查询（Generator + Guard + Executor） |
| `POST` | `/query/full` | 完整管线（全部 9 个节点） |
| `POST` | `/query/full/stream` | 完整管线 + SSE 流式输出 |

### 评测

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/eval/start` | 启动 BIRD 评测 |
| `GET` | `/eval/status/{id}` | 评测任务状态 |
| `GET` | `/eval/tasks` | 列出所有评测任务 |
| `DELETE` | `/eval/cancel/{id}` | 取消评测任务 |

### 健康

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/health` | 服务健康检查 |
| `GET` | `/schema` | 数据库 Schema（DDL + 表目录） |
| `GET` | `/databases` | 列出可用数据库 |

## BIRD Mini-Dev 评测结果

**最新**: 500 题全量，DeepSeek V4 Pro，R4 配置（RAG + MultiCandidate + Fewshot + ColumnPrune）。500 次调用 0 crash。

| 指标 | R4_PruneFewshot | R5_EvidenceFeedback |
|------|:---------------:|:-------------------:|
| **EX** | **37.6%** | **42.4%** |
| VES | 0.40 | — |
| 平均耗时 | 14.1s | — |
| 平均 Token | 16,284 | — |
| RAG 表召回 | **97.7%** (480) | — |
| Self-Correction 修复率 | 24.4% | — |
| Evidence Feedback 修复 | — | 24/311 (7.7%) |
| 成本（500 次） | $2.47 | — |

*R5_EvidenceFeedback: 先用 R4 跑，对 EX=0 的题目把 BIRD evidence 当作 user_feedback 通过 feedback graph 修复。*

### 按难度分层 (R4)

| 简单 (148) | 中等 (250) | 困难 (102) |
|:----------:|:----------:|:----------:|
| 50.0% | 36.8% | 21.6% |

### 模块分析 (R4, 500 题)

| 模块 | 指标 | 数值 |
|------|------|------|
| **Guard** | 拒绝率 / 假阴性率 | 2.5% / 58.5% |
| **SemCheck** | 拒绝率 / 假阴性率 | 24.1% / 39.7% |
| **Self-Correction** | 重试率 / 修复率 | 53.2% / 24.4% |
| **Voter** | 多候选 / 平均候选 | 266 / 1.83 |
| **Decomposer** | 复杂题 EX | 27.5% (149 题) |
| **Evidence Feedback** | 尝试 / 修复 | 311 / 24 (7.7%) |

### 核心发现

1. **RAG 是最大杠杆** — 较基线提升 +11pp
2. **强模型效果显著** — Claude Opus 4.7 EX 47.0% vs DeepSeek 39.0%（+8pp，100 题抽测）
3. **SemCheck 是最大优化机会** — FN 39.7%，降低可带来 +4-5pp
4. **Decomposer 差距依旧** — 复杂题 EX 27.5% vs 简单题 50.0%（22.5pp）
5. **Evidence post-hoc 反馈** — evidence 作为 user_feedback 修复 7.7% → EX 37.6% → 42.4%

## 技术栈

| 层 | 选型 |
|----|------|
| LLM | DeepSeek V4 Pro / OpenAI GPT-4o / Claude Opus 4.7 |
| Embedding | BAAI/bge-small-zh-v1.5（本地） |
| 向量库 | ChromaDB |
| 编排 | LangGraph + LangChain |
| AST 校验 | sqlglot (Python) |
| 数据库 | SQLite / PostgreSQL 16 / MySQL 8.4 |
| 缓存+状态 | Redis 7 |
| 消息队列 | Kafka 3.7（KRaft 模式，无需 ZooKeeper） |
| API | FastAPI + Pydantic v2 |
| MCP 协议 | fastmcp (Python) |
| 网关 | Spring Boot 3.3 / Java 21 |
| 前端 | Streamlit 1.51 |
| 可观测 | OpenTelemetry + TraceLogger (jsonl) |
| 部署 | Docker Compose（6 服务） |

## 项目结构

```
nl2sql-agent/
  agent/                   # LangGraph 节点 + 图 + 状态
    nodes/                 # router, generator, guard, voter, refiner 等
    graphs/                # full_graph, feedback_graph
  api/                     # FastAPI 应用 + 路由 (task, query, eval, health)
  worker/                  # Kafka 消费者（独立进程）
  infrastructure/          # broker (Kafka) + task_store (Redis 状态机)
  guard/                   # safety_rules (9 规则) + error_types + error_classifier
  tools/
    sql_executor.py        # SQL 执行引擎
  gateway-java/            # Spring 网关 (代理 + 熔断 + 指标)
  gateway/                 # [M1 已退役] Go 网关, 仅为参照保留, M4 后删除
  retrieval/               # RAG 管线 (schema + domain + sample rows)
  observability/           # TraceLogger + OTel 桥接
  evaluation/              # BIRD 评测运行器
  storage/                 # Redis 缓存 + config + db_registry
  configs/                 # Prompt 版本 + 系统提示
  corpus/bird_fewshot/     # Few-shot 示例 (按 db_id + 方言)
  deployment/              # Dockerfile + docker-compose.yml
  tests/                   # 14 个测试文件 (188+ 测试)
  ui/                      # Streamlit 应用 (ChatGPT 风格)
```

## 评测命令

```bash
# 快速测试（20 题，2 个配置）
python -m evaluation.run --test --samples 20 --configs R2,R5

# 完整消融矩阵（500 题，8 并发，断点续跑）
python -m evaluation.run --exp ablation --max-workers 8

# 预计算 Gold SQL 缓存
python -m evaluation.precompute_gold

# 多数据库冒烟
python tests/smoke_multidb.py

# 运行全部测试
python -m pytest tests/ -v
```

## LLM 模型配置

Streamlit 侧边栏 4 个预设。密钥在 `llm_keys.json`（不提交 Git）：

```json
{"deepseek": "sk-xxx", "openai": "sk-xxx", "anthropic": "sk-ant-xxx"}
```

| Provider | Model | 说明 |
|----------|-------|------|
| DeepSeek V4 Pro | deepseek-v4-pro | 默认，兼容 OpenAI SDK |
| OpenAI GPT-4o | gpt-4o | 标准 OpenAI API |
| Claude Opus 4.7 | claude-opus-4-7 | 自动切换 ChatAnthropic |
| Custom | 自定义 | 兼容 OpenAI 的 API |

## 接入自有数据库

编辑 `databases.json`：

```json
{
  "databases": [
    {"db_id": "my_mysql", "display_name": "生产 MySQL",
     "database_url": "mysql+pymysql://user:pass@host:3306/db"},
    {"db_id": "my_pg", "display_name": "分析 PG",
     "database_url": "postgresql+psycopg2://user:pass@host:5432/db"}
  ]
}
```

方言、表数量、在线状态自动检测。保存后刷新页面即可在下拉框中选择。

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `LLM_BASE_URL` | `https://api.deepseek.com/v1` | LLM API 地址 |
| `LLM_CHAT_MODEL` | `deepseek-v4-pro` | 默认模型 |
| `LLM_API_KEY` | （从 llm_keys.json 或 env 读取） | API 密钥 |
| `DATABASE_URL` | `sqlite:///./data/demo.db` | 默认数据库 |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis 连接 |
| `EMBED_MODEL_NAME` | `BAAI/bge-small-zh-v1.5` | Embedding 模型 |
| `CHROMA_PATH` | `./data/chroma_db` | ChromaDB 路径 |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka Broker |
