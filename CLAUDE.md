# CLAUDE.md — NL2SQL Agent v0.2.0

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

## 技术栈

| 层 | 选型 |
|----|------|
| LLM | DeepSeek API (ChatOpenAI 兼容) |
| Embedding | BAAI/bge-small-zh-v1.5 (sentence-transformers, 本地) |
| 向量库 | ChromaDB 本地持久化 |
| 编排 | LangGraph + LangChain 1.2+ |
| DB | SQLite / PostgreSQL / MySQL (SQLAlchemy 2.0) |
| 接口 | FastAPI + Streamlit |
| 缓存 | Redis (LLM 语义缓存 + Schema 元数据缓存) |

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
    schema.py              # Schema 反射 + 缓存 + get_engine
    execute.py             # SQL 执行沙箱
    generate.py            # Generator（兼容重导出 → src/prompts.py）
    rag_retrieve.py        # ChromaDB 检索
    pipeline.py, config.py # Mini 版兼容
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
    prompts.py             # 所有 LLM prompt 常量（5 个）
    retrieval/             # RAG 检索管线
    api/                   # FastAPI 接口
    infrastructure/        # Redis 缓存
    obs/                   # TraceLogger 结构化日志
    guardrails/            # AST 校验 (sqlglot)
  scripts/
    eval_bird.py           # 主评测脚本（--test / --exp ablation）
    _precompute_gold.py    # Gold SQL 预计算缓存
    ingest_bird.py         # BIRD schema 向量化
  reports/
    .gold_cache/           # Gold SQL 预计算结果（JSON, 按 DB 分文件）
    bird_ablation_*_summary.{md,json}  # 消融报告
    checkpoint_ablation.json          # 断点续跑
  logs/traces/             # 流式 trace (jsonl, 即时 fsync)
  data/bird/mini_dev_data/ # BIRD Mini-Dev 数据集
```

## 环境变量

| 变量 | 默认值 |
|------|--------|
| LLM_BASE_URL | https://api.deepseek.com/v1 |
| LLM_CHAT_MODEL | deepseek-v4-pro |
| SQL_DIALECT | sqlite |
| DATABASE_URL | sqlite:///./data/demo.db |
| EMBED_MODEL_NAME | BAAI/bge-small-zh-v1.5 |

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
2. **Guard FN 率高 (49-68%)**：幻觉检测太松，硬规则覆盖不足
3. **SemCheck FN 率高 (50-65%)**：LLM 语义审查漏检率高，prompt 需要进一步优化
4. **Generator 时间占比 50%+**：多候选时翻倍（2.5→6.0s），考虑 speculative decoding 或 batched generation
5. **Decomposer 对 DeepSeek 无效**：模型不具备拆解能力时禁用更优

## 编码约定

- SQLAlchemy 2.0：`with engine.connect() as conn:`，不用 `engine.execute()`
- 文件读写显式 `encoding='utf-8'`
- 所有 LLM prompt 从 `src/prompts.py` 引用，不在节点内硬编码
- Schema 访问统一走 `_get_cached_schema_info()`，禁止绕过
- Pool/Executor 超时后必须 `shutdown(wait=False)`
- TraceLogger 节点：`tlog.node_enter/exit`，LLM 调用：`tlog.llm_call/llm_error`
