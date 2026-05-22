<p align="right"><a href="README.md">English</a></p>

# NL2SQL Agent

基于 LangGraph 的自然语言转 SQL 系统，集成 RAG 增强的 Schema 检索与 Self-Correction 自修复执行循环。

## 架构

```
                         ┌─────────────────────┐
                         │   Schema Retriever  │
                         │   RAG 检索 + DDL    │
                         └──────────┬──────────┘
                                    │
                         ┌──────────▼──────────┐
                         │       Router        │
                         │  简单 / 复杂 判定    │
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
                       │   多候选生成          │
                       │  (temp 0/0.3/0.6)  │
                       └──────────┬──────────┘
                                  │
                       ┌──────────▼──────────┐
                       │        Guard        │
                       │  语法检查 + 幻觉检测  │
                       └──────────┬──────────┘
                                  │
                        通过 ─────┴───── 不通过
                          │                │
               ┌──────────▼──────────┐  ┌──▼──────────┐
               │        Voter        │  │   Refiner   │
               │  并行执行 +          │  │  错误 →     │
               │  LLM 兜底投票        │  │  Generator  │
               └──────────┬──────────┘  └─────────────┘
                          │
                 有优胜 ───┴─── 无优胜 → Refiner
                          │
               ┌──────────▼──────────┐
               │   Semantic Check    │
               │   LLM 二元语义判定   │
               └──────────┬──────────┘
                          │
                  通过 ───┴─── 不通过 → Refiner
                          │
                         END
```

## 快速开始

```bash
pip install -r requirements.txt
cp .env.example .env        # 填入 LLM_API_KEY
python scripts/ingest.py    # 构建 RAG 索引
streamlit run app.py        # 启动前端（http://127.0.0.1:8501）
```

Docker 部署（含 MySQL + PostgreSQL Demo 数据库）：

```bash
docker compose up -d                 # 启动全部服务
python scripts/_smoke_multidb.py     # 多数据库冒烟测试（9 题 × 3 方言）
```

## LLM 多模型切换

Streamlit 侧边栏 Provider 下拉框支持 4 个预设，自动填充模型名和 API 地址：

| Provider | Model | 说明 |
|----------|-------|------|
| DeepSeek V4 Pro | deepseek-v4-pro | 默认，兼容 OpenAI SDK |
| OpenAI GPT-4o | gpt-4o | 需 OpenAI API Key |
| Claude Opus 4.7 | claude-opus-4-7 | 底层自动切换 ChatAnthropic |
| Custom | 自定义 | 兼容任意 OpenAI 兼容 API |

密钥文件 `llm_keys.json`（不提交 Git）：
```json
{"deepseek": "sk-xxx", "openai": "sk-xxx", "anthropic": "sk-ant-xxx"}
```

## 接入自己的数据库

编辑 `databases.json`，系统自动检测方言、表数量、在线状态：

```json
{
  "databases": [
    {"db_id": "my_mysql", "display_name": "生产 MySQL",
     "database_url": "mysql+pymysql://user:pass@host:3306/db"},
    {"db_id": "my_pg", "display_name": "分析 PG",
     "database_url": "postgresql+psycopg2://user:pass@host:5432/db"},
    {"db_id": "docker_mysql", "display_name": "Docker MySQL Demo",
     "database_url": "mysql+pymysql://nl2sql:nl2sql@127.0.0.1:3306/demo"},
    {"db_id": "docker_pg", "display_name": "Docker PG Demo",
     "database_url": "postgresql+psycopg2://nl2sql:nl2sql@127.0.0.1:5432/demo"}
  ]
}
```

Docker 环境可快速启动 Demo 数据库：`docker compose up -d mysql postgres`

保存后刷新 Streamlit 页面，数据库自动出现在下拉框，离线数据库标记 "(offline)"。

## API 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/v1/query` | Mini 管线（单次生成+自修复） |
| `POST` | `/api/v1/query/full` | Full LangGraph 管线（含 Vote+SemCheck） |
| `POST` | `/api/v1/query/full/stream` | Full 管线 SSE 流式（节点级进度推送） |
| `GET` | `/api/v1/health` | 健康检查 |
| `GET` | `/api/v1/schema` | Schema DDL + 表目录 |

```json
// POST /api/v1/query/full
{
  "question": "查询销售额最高的 5 个产品",
  "db_id": "mysql_demo",
  "database_url": "mysql+pymysql://nl2sql:nl2sql@127.0.0.1:3306/demo",
  "llm": {"model": "deepseek-v4-pro", "api_key": "sk-xxx", "base_url": "https://api.deepseek.com/v1"}
}
```

## 多数据库支持

| 方言 | 连接串 | 状态 |
|------|--------|------|
| SQLite | `sqlite:///./data/demo.db` | 内置 BIRD 11 库 |
| MySQL | `mysql+pymysql://user:pass@localhost:3306/demo` | 用户 JSON |
| PostgreSQL | `postgresql+psycopg2://user:pass@host:5432/demo` | 用户 JSON |

从连接串自动识别方言，注入对应 Few-shot 示例（`corpus/bird_fewshot/mysql.md`、`postgresql.md`），切换 AST 校验方言，适配错误分类规则。

## BIRD Mini-Dev 评测结果

500 题，11 个数据库，Full LangGraph 管线逐模块消融。

| 配置 | EX | VES | 耗时 | 说明 |
|--------|-----|-----|------|-------------|
| R0_Baseline | 23.4% | 0.334 | 6.98s | 纯 Generator |
| R1_Decomposer | 23.8% | 0.374 | 5.80s | + 问题拆解 |
| R2_RAG | **34.6%** | **0.506** | 5.06s | + RAG（性价比最高） |
| R3_MultiCandidate | 34.0% | 0.376 | 9.45s | + 多候选投票 |
| R4_PruneFewshot | 37.4% | 0.353 | 10.75s | + 列剪枝 + Few-shot |
| R5_Evidence | **38.8%** | 0.303 | 12.88s | + BIRD 人工 evidence |

### 按难度分层

| 配置 | 简单 (148) | 中等 (250) | 困难 (102) |
|--------|:----------:|:----------:|:-----------:|
| R0_Baseline | 37.8% | 20.8% | 8.8% |
| R2_RAG | 48.6% | 30.8% | 23.5% |
| R4_PruneFewshot | 52.0% | 36.0% | 19.6% |
| R5_Evidence | 49.3% | 38.0% | 25.5% |

## 评测命令

```bash
# 快速测试（20 题）
python scripts/eval_bird.py --test --samples 20 --configs R2,R5

# 完整消融矩阵（500 题，支持断点续跑）
python scripts/eval_bird.py --exp ablation --max-workers 8

# 预计算 gold SQL 缓存（一次性）
python scripts/_precompute_gold.py
```

## 技术栈

| 层 | 选型 |
|----|------|
| LLM | DeepSeek / OpenAI / Claude |
| Embedding | BAAI/bge-small-zh-v1.5（本地） |
| 向量库 | ChromaDB |
| 编排 | LangGraph + LangChain |
| 数据库 | SQLite / PostgreSQL / MySQL |
| AST 校验 | sqlglot（自动检测方言） |
| API | FastAPI + Pydantic |
| 缓存 | Redis |
| 前端 | Streamlit |

## 环境变量

| 变量 | 默认值 |
|------|--------|
| `LLM_BASE_URL` | `https://api.deepseek.com/v1` |
| `LLM_CHAT_MODEL` | `deepseek-v4-pro` |
| `LLM_API_KEY` | （从 llm_keys.json 或环境变量读取） |
| `DATABASE_URL` | `sqlite:///./data/demo.db` |
| `REDIS_URL` | `redis://localhost:6379/0` |
| `EMBED_MODEL_NAME` | `BAAI/bge-small-zh-v1.5` |
