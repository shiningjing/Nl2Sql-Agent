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
  Go API Gateway (:8080)          限流 · 健康检查 · 反向代理
        │
        ▼
  FastAPI  (:8000) ──────────────────────────────┐
        │                                         │
        │  submit ──→ Kafka ──→ Worker            │
        │  feedback ──→ Kafka ──→ Worker          │
        │                                         │
        │  SSE ←── Redis (轮询状态 + Pub/Sub) ◄───┘
        │
        │  /query/full/stream: FastAPI 线程内运行 LangGraph
        │  （同步路径 — 无需 Kafka / Worker）
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
| **Go API 网关** | go-chi 路由 · 滑动窗口限流（100 req/min/IP）· 聚合健康检查 · 结构化日志 |
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

**最新**: 100 题，DeepSeek V4 Pro，Full Graph + RAG。200 次调用 0 crash。

| 指标 | R2_RAG | R5_Evidence |
|------|--------|-------------|
| **EX** | **39.0%** | **43.0%** |
| VES | 0.49 | 0.43 |
| 平均耗时 | 11.6s | 10.9s |
| 平均 Token | 16,762 | 15,358 |
| RAG 表召回 | **98.4%** | 97.9% |
| Self-Correction 修复率 | 26.5% | 24.1% |
| 成本（200 次） | — | $0.97（¥7） |

### 按难度分层

| 配置 | 简单 (37) | 中等 (49) | 困难 (14) |
|------|:---------:|:---------:|:---------:|
| R2_RAG | 46.0% | 34.7% | 35.7% |
| R5_Evidence | 48.6% | 38.8% | 42.9% |

### 模块分析（R2_RAG）

| 模块 | 指标 | 数值 |
|------|------|------|
| **Guard** | 假阴性率 | 55.6%（纯形式校验，无语义能力） |
| **SemCheck** | 假阴性率 | 38.9%（LLM 判 YES 但 EX=0） |
| **Self-Correction** | 重试率 / 修复率 | 49.0% 重试 · 26.5% 修复 |
| **Voter** | 单候选 / 多候选 / 平票 | 26 / 4 / 19（共 49 次重试） |
| **Decomposer** | 复杂题 EX | 26.7%（30 道复杂题） |

### 核心发现

1. **RAG 是最大杠杆** — 较基线提升 +11pp（23.4% → 34.6%）
2. **强模型效果显著** — Claude Opus 4.7 EX 47.0% vs DeepSeek 39.0%（+8pp）
3. **Decomposer 对 DeepSeek 无效** — 复杂题 EX 低于简单题
4. **Self-Correction** 修复率从 7-20% 提升至 **24-27%**
5. **Guard 是形式校验** — 55% 通过的 SQL 仍然产生错误结果

## 技术栈

| 层 | 选型 |
|----|------|
| LLM | DeepSeek V4 Pro / OpenAI GPT-4o / Claude Opus 4.7 |
| Embedding | BAAI/bge-small-zh-v1.5（本地） |
| 向量库 | ChromaDB |
| 编排 | LangGraph + LangChain |
| AST 校验 | sqlglot (Python) + vitess/sqlparser (Go) |
| 数据库 | SQLite / PostgreSQL 16 / MySQL 8.4 |
| 缓存+状态 | Redis 7 |
| 消息队列 | Kafka 3.7（KRaft 模式，无需 ZooKeeper） |
| API | FastAPI + Pydantic v2 |
| MCP 协议 | fastmcp (Python) + mark3labs/mcp-go (Go) |
| 网关 | go-chi/chi (Go) |
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
    mcp/                   # Python MCP 工具 (validate_sql, execute_readonly_sql)
    mcp-server-go/         # Go MCP Server (vitess/sqlparser + database/sql)
    sql_executor.py        # SQL 执行引擎
  gateway/                 # Go API 网关 (go-chi 限流 + 反向代理)
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
