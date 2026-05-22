# CLAUDE.md — NL2SQL Agent v0.2.1

## 项目概览

自然语言 → SQL 端到端系统。LangGraph 状态机编排，Router → Schema Retriever → Decomposer → Generator → Guard → Voter → SemCheck → Refiner 循环。BIRD Mini-Dev（500 题，11 数据库，3 方言）消融评测。

**BIRD 消融结果 (2026-05-21)**：

| Config | EX | VES | 耗时 | 要点 |
|--------|-----|-----|------|------|
| R0_Baseline | 23.4% | 0.334 | 6.98s | 裸 Generator |
| R1_Decomposer | 23.8% | 0.374 | 5.80s | +拆解（净负收益） |
| R2_RAG | 34.6% | 0.506 | 5.06s | +RAG 最大跳跃 +11pp |
| R3_MultiCandidate | 34.0% | 0.376 | 9.45s | +多候选（反降） |
| R4_PruneFewshot | 37.4% | 0.353 | 10.75s | +列剪枝+Fewshot |
| R5_Evidence | **38.8%** | 0.303 | 12.88s | BIRD 人工 evidence 天花板 |

**核心发现**：RAG 是最大杠杆 (+11pp)；Decomposer 对 DeepSeek 几乎无效；MultiCandidate 不值得；Self-Correction 修复率 7-20% 是最大瓶颈。

## 版本历史

- **v0.1.0** (W1-W8): Mini 闭环 + LangGraph + BIRD 评测基础设施
- **v0.2.0** (后 BIRD 优化): 34 项修复（并发安全、Prompt 优化、Gold Cache、超时治理），R0→R5 完整消融数据
- **v0.2.1** (2026-05-22): 多模型支持（DeepSeek/OpenAI/Claude）、MySQL/PG 全链路适配、用户数据库导入、多方言 Few-shot、Guard 双引号修复、Decimal 序列化修复

## 技术栈

| 层 | 选型 |
|----|------|
| LLM | DeepSeek / OpenAI / Claude (ChatOpenAI + ChatAnthropic) |
| Embedding | BAAI/bge-small-zh-v1.5 (sentence-transformers, 本地) |
| 向量库 | ChromaDB 本地持久化 |
| 编排 | LangGraph + LangChain 1.2+ |
| DB | SQLite / PostgreSQL / MySQL (SQLAlchemy 2.0) |
| AST 校验 | sqlglot (自动检测方言) |
| 接口 | FastAPI + Streamlit |
| 缓存 | Redis (LLM 语义缓存，本地优先 → Docker 自动回退) |

## 关键约束

1. SQL 只允许 SELECT（含 WITH），其他 DML/DDL 一律拒绝
2. 所有 Schema 消费者统一走 `_get_cached_schema_info()`（`nl2sql/schema.py`），禁止 raw `inspect()`
3. Generator temperature=0（确定性），多候选用 0/0.3/0.6 + 去重早停
4. LIMIT 默认 200，硬上限 1000
5. SQLite DB 连接自动 `PRAGMA journal_mode=WAL`（并发 SELECT 不排队）
6. ThreadPoolExecutor 超时后用 `pool.shutdown(wait=False)` 防卡死
7. 所有 prompt 统一定义在 `src/prompts.py`

## 项目结构

```
nl2sql-mini-agent/
  nl2sql/                  # 核心库
    schema.py              # Schema 反射 + 缓存 + get_engine（WAL 仅 SQLite）
    execute.py             # SQL 执行沙箱
    generate.py            # Generator + get_dialect_from_url()
    db_registry.py         # 数据库注册中心（BIRD + Docker + 用户 JSON）
    rag_retrieve.py        # ChromaDB 检索
    pipeline.py            # 错误分类（11 种模式，3 方言）
    config.py              # LLM_PRESETS + llm_keys.json 读写
    review.py              # Review prompt（动态方言）
  src/
    agent/
      state.py             # AgentState TypedDict
      graphs/full_graph.py # LangGraph 主图（条件边+循环）
      nodes/
        router.py, schema_retriever.py, decomposer.py
        fewshot_selector.py, generator.py, guard.py
        voter.py, executor.py, semantic_check.py, refiner.py
    eval/
      bird_loader.py       # BIRD 数据加载 + DB URL
      metrics.py           # exec_match, VES, normalize_rows
      task_manager.py      # 异步评测编排
    prompts.py             # 所有 LLM prompt 常量
    retrieval/             # RAG 检索管线（fk_expand, column_prune, fewshot）
    api/                   # FastAPI 接口（/api/v1/query, /query/full, /query/full/stream）
    infrastructure/        # Redis 语义缓存（本地优先 + Docker 回退）+ LLM 工厂
    obs/                   # TraceLogger 结构化日志
    guardrails/            # AST 校验 (sqlglot)
  corpus/bird_fewshot/     # Few-shot 示例（按 db_id + 方言）
    mysql.md               # MySQL 方言示例（7 对）
    postgresql.md          # PostgreSQL 方言示例（7 对）
    california_schools.md  # BIRD 数据库示例（11 个）
    ...
  scripts/
    eval_bird.py           # 主评测脚本（--test / --exp ablation）
    _precompute_gold.py    # Gold SQL 预计算缓存
    _smoke_multidb.py      # 多数据库冒烟测试（9 题 × 3 方言）
    _show_demo_tables.py   # Demo 数据库表结构查看
    ingest_bird.py         # BIRD schema 向量化
  reports/
    .gold_cache/           # Gold SQL 预计算结果（JSON, 按 DB 分文件）
    bird_ablation_*_summary.{md,json}  # 消融报告
    checkpoint_ablation.json          # 断点续跑
  logs/traces/             # 流式 trace (jsonl, 即时 fsync)
  data/bird/mini_dev_data/ # BIRD Mini-Dev 数据集
  llm_keys.json            # LLM 密钥（不提交 Git）
  databases.json           # 用户自定义数据库连接
```

## 环境变量

| 变量 | 默认值 |
|------|--------|
| LLM_BASE_URL | https://api.deepseek.com/v1 |
| LLM_CHAT_MODEL | deepseek-v4-pro |
| LLM_API_KEY | (从 llm_keys.json 或环境变量读取) |
| SQL_DIALECT | sqlite（已弃用，现在从 database_url 自动推导） |
| DATABASE_URL | sqlite:///./data/demo.db |
| EMBED_MODEL_NAME | BAAI/bge-small-zh-v1.5 |
| REDIS_URL | redis://localhost:6379/0 |

## LLM 多模型支持

通过 Streamlit 侧边栏 Provider 下拉框切换，或 API 请求中传 `llm` 字段。预设了 4 个 Provider：

| Provider | Model | Base URL |
|----------|-------|----------|
| DeepSeek V4 Pro | deepseek-v4-pro | https://api.deepseek.com/v1 |
| OpenAI GPT-4o | gpt-4o | https://api.openai.com/v1 |
| Claude Opus 4.7 | claude-opus-4-7 | https://api.anthropic.com |
| Custom | (任意) | (任意) |

密钥统一存在 `llm_keys.json`（不提交 Git），格式：
```json
{"deepseek": "", "openai": "", "anthropic": ""}
```

底层自动检测 Anthropic URL/Model 名，映射到 `ChatAnthropic` 对应的参数名（`anthropic_api_key`、`anthropic_api_url`、`default_request_timeout`）。

## 用户自定义数据库

编辑 `databases.json` 添加自己的数据库连接，自动出现在 Streamlit 下拉框：

```json
{
  "databases": [
    {"db_id": "my_mysql", "display_name": "我的 MySQL",
     "database_url": "mysql+pymysql://user:pass@host:3306/db"},
    {"db_id": "my_pg", "display_name": "我的 PostgreSQL",
     "database_url": "postgresql+psycopg2://user:pass@host:5432/db"}
  ]
}
```

系统自动检测方言、表数量、在线状态。离线数据库在 UI 上标记 "(offline)"。

## 多数据库冒烟测试

```bash
# 需要先启动 Docker 容器（MySQL + PG）
docker compose up -d mysql postgres

# 运行冒烟测试（9 题 × 3 方言）
python scripts/_smoke_multidb.py
```

Docker Demo 数据库连接：
- MySQL: `mysql+pymysql://nl2sql:nl2sql@127.0.0.1:3306/demo`
- PG: `postgresql+psycopg2://nl2sql:nl2sql@127.0.0.1:5432/demo`

## 评测命令

```bash
# 测试模式（抽 N 题，指定配置）
python scripts/eval_bird.py --test --samples 20 --configs R2,R5

# 完整消融（支持断点续跑）
python scripts/eval_bird.py --exp ablation --max-workers 8

# 预计算 gold cache（首次或 gold SQL 变更时跑一次）
python scripts/_precompute_gold.py
```

## 评测矩阵

| Config | Decomposer | RAG | Multi-Candidate | Prune+Fewshot | Knowledge |
|--------|------------|-----|-----------------|---------------|-----------|
| R0_Baseline | | | | | rag |
| R1_Decomposer | ✅ | | | | rag |
| R2_RAG | ✅ | ✅ | | | rag |
| R3_MultiCandidate | ✅ | ✅ | ✅ | | rag |
| R4_PruneFewshot | ✅ | ✅ | ✅ | ✅ | rag |
| R5_Evidence | ✅ | ✅ | ✅ | ✅ | evidence |

## 已知瓶颈与优化方向

1. **Self-Correction 修复率极低 (7-20%)**：Refiner prompt 和上下文传递需要重新设计

2. **Guard FN 率高 (49-68%)** — 设计边界问题，非 bug。Guard 三层检查全是形式验证：
   - ① SELECT + 无禁用关键字 ② schema 标识符存在性 ③ sqlglot AST 语法
   - 2026-05-22 深潜 11 样本：**11/11 全部 FN**
   - **根因**：任何语法正确且标识符存在的 SQL 都会通过，Guard 无语义能力
   - **已修复 (v0.2.1)**：双引号标识符（`"County Name"`）的 FP 误报 — 正则未 strip 导致内部单词被当成裸标识符
   - **方向**：加硬规则层（关键词→语法要求映射），如 "top N" → 检查是否有 ORDER BY

3. **SemCheck FN 率高 (50-65%)** — 三个根因叠加：
   - ① Prompt `Default to YES. Only say NO when you have CONCRETE evidence` 过于宽松
   - ② `max_tokens=80` 无推理空间
   - ③ 无 gold 参照，LLM 从零判断语义正确性能力不足
   - **方向**：去 "default to YES" → 分步检查 prompt、max_tokens 提至 200-300

4. **Generator 时间占比 50%+**：多候选时翻倍（2.5→6.0s），考虑 speculative decoding

5. **Decomposer 对 DeepSeek 无效**：模型不具备拆解能力时禁用更优

6. **MySQL Decimal 类型** (v0.2.1 已修复)：`cache_set_llm` 中 `json.dumps` 无法序列化 MySQL DECIMAL 字段返回的 Python `Decimal` 对象，`_sanitize_exec_result` 已加 `Decimal → float` 转换，`redis_cache.py` 加 `default=str` 兜底。

## Redis 连接策略

`get_redis()` 本地优先 + Docker 自动回退：

1. 若 `REDIS_URL` 环境变量非默认值 → 直接用（Docker Compose 内会设 `redis://redis:6379/0`）
2. 否则自动探测：`127.0.0.1:6379` → `redis:6379`
3. 全部失败 → 返回 None，所有缓存操作静默降级为 no-op

已删除的死代码（v0.2.1）：`cache_get_schema` / `cache_set_schema` / `cache_get_table_catalog` / `cache_set_table_catalog` — Schema 缓存定义了但从未接入调用链，实际 `_get_cached_schema_info()` 走的是本地 `lru_cache`。

## 编码约定

- SQLAlchemy 2.0：`with engine.connect() as conn:`，不用 `engine.execute()`
- 文件读写显式 `encoding='utf-8'`
- 所有 LLM prompt 从 `src/prompts.py` 引用，不在节点内硬编码
- Schema 访问统一走 `_get_cached_schema_info()`，禁止绕过
- Pool/Executor 超时后必须 `shutdown(wait=False)`
- TraceLogger 节点：`tlog.node_enter/exit`，LLM 调用：`tlog.llm_call/llm_error`
