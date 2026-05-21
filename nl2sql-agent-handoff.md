# NL2SQL Agent with Self-Correction — 项目交接简报

> 复制本文给下一个编码 Agent / 协作者，用于对齐目标、流程与实现路径。

---

## 1. 项目目标

- **问题**：用户用自然语言提问，系统在真实数据库上自动生成可执行 SQL，并返回正确结果。
- **难点**：schema 规模大（表多列多）、幻觉列名 / 错误 JOIN、复杂问句需拆解。
- **方案**：以 **Agent 编排（推荐 LangGraph）** 为主干；用 **Schema RAG** 压缩上下文；用 **Self-Correction（执行反馈闭环）** 在 SQL 失败时用报错信息重写，最多重试 K 次（通常 3）。
- **产出**：可运行的代码仓库、可复现实验、评测表格（至少 EX + 可选 token/延迟）、简短 Demo（Streamlit / CLI）、README 含架构图。

---

## 2. 核心流程（逻辑管线）

**推荐主路径（多节点，可按工期删减）：**

1. **Schema Linking（RAG）**
   - **输入**：用户问题 `question`。
   - **输出**：与问题相关的 **Top-K 表/列** 的 schema 文本（DDL + 注释；可选 sample values）。
   - **实现选项**：向量检索（Chroma / Milvus）+ embedding（如 BGE-M3）；进阶可加 BM25 混合检索或 CHESS 类剪枝思路。

2. **（可选）Decomposer**
   - 复杂问题拆成子问题或子 SQL 计划（CoT / Plan-and-Solve）。工期紧可先跳过。

3. **SQL Generator**
   - **输入**：`question` + 检索到的 `schema` +（可选）few-shot 示例。
   - **输出**：候选 SQL（可先单候选；进阶可多 temperature 多候选）。

4. **Executor（沙箱）**
   - 在 **只读** 环境执行：`DuckDB attach SQLite` / PostgreSQL 只读账号 / 专用副本。
   - **捕获**：语法错误、未知列、类型错误、超时；可选检查空结果是否合理。

5. **Self-Correction / Refiner**
   - **输入**：`question`、`schema`、`previous_sql`、`error_message`（或执行反馈）。
   - **输出**：修正后的 SQL；`retry_count += 1`。
   - **条件边**：成功或达到 `max_retry` → 结束；否则回到 Executor。

6. **（可选）结果校验**
   - 与 gold 比对（评测模式）；或对结果做简单一致性检查（生产模式）。

**RAG 与 Correction 分工**：RAG 解决「上下文该带哪些表」；Correction 解决「SQL 错了怎么根据执行反馈改」。

---

## 3. 推荐技术栈（默认可改）

| 类别 | 建议 |
|------|------|
| 编排 | LangGraph（状态机 + 条件边，适合 retry 环） |
| LLM | LiteLLM 统一接口；开发期可用 DeepSeek / OpenAI / 通义等 |
| 向量库 | Chroma（轻量）或 Milvus（更重） |
| Embedding | BGE-M3（中英）或 OpenAI text-embedding-3 |
| 执行 | DuckDB（易嵌入）或目标库的只读实例 |
| 评测 | BIRD / Spider 官方或自建脚本；指标以 **Execution Accuracy (EX)** 为主 |

---

## 4. 数据与评测

- **数据集**：BIRD（更贴近真实大库）、Spider（经典）；可做 **dev 子集** 控制成本。
- **必做对比实验（写进 README）**：
  - Baseline：不用 RAG（全量或截断 schema）+ 无 Correction；
  - +Schema RAG；
  - +Self-Correction；
  - Full pipeline。
- **记录**：EX（及 BIRD 的 VES 若有余力）、平均 token、延迟、平均重试次数。

---

## 5. 实现路径（分两档）

### A. 速成 NL2SQL Mini（约 7～10 天）

- LangGraph：`schema_rag → generate → executor ↔ refiner`
- 单层 Schema RAG（表级文本 embedding + Top-K）
- 小评测集（例如 50 题）
- Streamlit 或 CLI Demo + README + 评测表

### B. 完整版（约 4～8 周）——详细规格

> 本节约定：**完整版 = Mini 能跑的闭环之上**，按模块逐项加厚；最终实现一套「可多 Agent、可观测、可复现实验矩阵」的 NL2SQL 系统。

#### B.0 与 Mini 的边界（必须对齐的预期）

| 维度 | Mini | 完整版 B |
|------|------|----------|
| Schema |  mostly **表级** RAG（每张表一段文本 Top-K） | **表 → 列** 两级 linking；必要时 **列级** 剪枝与消歧 |
| 问题复杂度 | 单段提问为主 | **Decomposer**：识别需拆分的长问题（JOIN 多层 / 嵌套聚合 / 多意图） |
| SQL 生成 | 单次生成为主 | **多候选**（不同 temperature / 不同 prompt）+ **一致性投票或执行验证择优** |
| 纠错 | 基于报错字符串重写 | 报错 + **空结果启发式** +（可选）**语法 AST 校验**；可选独立 **Critic** 节点 |
| 评测 | 小 dev 子集（如 50） | **分层评测**：按难度 / 按库大小 / 完整 dev 或更大子集；记录 **VES**（若 BIRD） |
| 工程 | README + 脚本 | **Docker Compose**、统一配置、**trace / metrics**、CI 跑 smoke eval |
| 差异化 | 可无 | **任选一条** 深做（见 B.7），写进论文或简历「创新点」 |

#### B.1 完整版逻辑架构（推荐多 Agent）

下列节点均可实现为 LangGraph 中的 **node**；Agent 之间通过 **共享 State** 通信（TypedDict / dataclass）。

```
用户 question + db_id
       │
       ▼
┌──────────────────┐
│ Router（可选）    │  判断：简单单表 / 复杂多跳 / 是否需要外部知识
└────────┬─────────┘
         ▼
┌──────────────────┐
│ Schema Retriever │  混合检索 Top-K 表；列级打分；外键图扩展（见 B.2）
└────────┬─────────┘
         ▼
┌──────────────────┐
│ Decomposer       │  输出：子问题列表 + 依赖关系（DAG）或线性步骤
└────────┬─────────┘
         ▼
┌──────────────────┐
│ Few-shot Selector│  （可选）从训练集 embedding 检索相似 (q, sql) 示例
└────────┬─────────┘
         ▼
┌──────────────────┐
│ SQL Generator    │  生成 N 条候选 SQL（N=3～5 常用）
└────────┬─────────┘
         ▼
┌──────────────────┐
│ Executor         │  沙箱执行；收集错误 / 结果集 hash / 行数
└────────┬─────────┘
         ▼
┌──────────────────┐
│ Selector/Voter   │  多候选择优：执行成功优先；成功则比较结果一致性投票
└────────┬─────────┘
         │ 失败或需改进
         ▼
┌──────────────────┐
│ Refiner          │  Self-Correction：喂入 error + 上一轮 SQL + schema 片段
└────────┬─────────┘
         │ retry < max_retry → 回到 Executor；否则 END（失败兜底）
         ▼
┌──────────────────┐
│ （可选）Critic   │  对「执行成功但可能语义错」做二次检查（LLM 比对 question）
└──────────────────┘
```

**状态 State 建议字段（完整版）**：`question`、`db_id`、`schema_compact`、`sub_questions`、`candidate_sqls[]`、`chosen_sql`、`exec_error`、`result_signature`（如规范化后的结果 hash）、`retry_count`、`trace_id`、`token_usage`。

#### B.2 Schema Linking（完整版要做的「更强」具体指什么）

1. **混合检索（Hybrid）**
   - **稠密**：表/列描述 embedding，与 question 相似度。
   - **稀疏**：BM25 打表名、列名、注释（对短 token 如 `id`、`avg` 更稳）。
   - **融合**：RRF（Reciprocal Rank Fusion）或加权分数；输出 Top-K_table。

2. **外键图扩展（Schema Graph Expansion）**
   - 在 Top-K_table 基础上，沿 **FK 边** 扩展 1-hop（避免漏 JOIN 路径）。
   - 实现：预解析 `PRAGMA foreign_key_list` / information_schema。

3. **列级 Linking（可选但完整版强烈建议）**
   - 第二阶段：只对入选表内的列做打分（向量 + 关键词），剔除无关列，进一步压缩 prompt。
   - 产出：`schema_compact`：仅包含「被选表 + 被选列 + PK/FK」。

4. **值检索（可选，针对 BIRD 类 dirty value）**
   - 对枚举型列建 **mini 倒排**：Cell 值 → 列名；question 中的实体先映射到列取值提示。
   - 注意隐私与体积：可用哈希截断或仅 Top 频次值。

5. **与 Self-Correction 联动**
   - 若错误为 `no such column`，Refiner 可触发 **「扩大 K」或「二次检索列」**（narrow→widen 策略），再生成 SQL。

#### B.3 Decomposer（完整版规格）

- **触发条件**：Router 或启发式（question 长度、关键词 `and/then/each/compare`、子句数量）。
- **输出格式（机器可读）**：
  - `steps: [{ id, sub_question, depends_on[] }]`  
  - 或线性 `CTE 计划`：`WITH s1 AS (...), s2 AS (...)`。
- **执行策略**：
  - **顺序执行**：每步子 SQL 的结果可写入临时视图（DuckDB）或内存 DataFrame，下一步引用。
  - **单次合并**：有时 LLM 可直接输出带 CTE 的单条 SQL——Decomposer 只负责「规划」，生成仍交给 Generator。
- **失败处理**：若拆解不合理，Refiner 可退回「不拆解」路径重试。

#### B.4 SQL 生成与多候选择优（完整版规格）

1. **多候选生成**
   - 同一 prompt + `temperature ∈ {0.2, 0.7, 1.0}`；或 **prompt 变体**（强调 JOIN / 强调聚合）。
   - 输出 `candidate_sqls[N]`。

2. **执行过滤**
   - 每条在沙箱执行；丢弃执行失败。
   - 成功多条：对 **结果集规范化**（排序列、round）后比较 hash；**多数投票**选一致结果。
   - 若结果均不一致：选 **代价最小**（见 B.7 EXPLAIN 可选）或交 Refiner。

3. **与评测对齐**
   - 评测脚本中对 gold SQL 执行得到 `gold_hash`，与候选结果比对 EX。

#### B.5 Executor / 沙箱（完整版加固项）

- **只读**：连接串带 read-only；SQLite 用 `mode=ro`。
- **超时**：单查询 wall-clock 上限（如 5～30s，按库调节）。
- **资源**：限制返回行数（`LIMIT` 注入策略要谨慎，更适合在执行层截断）。
- **方言**：统一目标方言（SQLite）；若在 PG，要在 prompt 里固定方言说明。
- **AST 校验（可选）**：用 `sqlglot` 解析生成 SQL；拒绝多语句、拒绝危险关键字（按白名单）。

#### B.6 Self-Correction / Refiner（完整版加固项）

- **输入**：除 error 外，增加 `failed_sql`、`execution_log`、`schema_compact`、`sub_plan`（若有）。
- **策略分级**：
  - Level 1：Syntax / semantic error → 直接重写。
  - Level 2：空结果 → LLM 判断是否「合理为空」；否则提示检查 JOIN/WHERE。
  - Level 3：（可选）Critic 怀疑语义不符 → 触发重写或触发 **扩大 schema 检索**。
- **终止条件**：`max_retry`（建议 3～5）、或连续两次相同错误退出（防死循环）。

#### B.7 可选差异化方向（完整版选一条深做）

以下为 **「简历 / 论文加分」** 模块，与核心流水线 **插件化** 接入（勿一次性全做）。

1. **RL-based Refiner（与 DRL 背景契合）**
   - 将「纠错动作」离散化：重写 JOIN / 改 WHERE / 换聚合 / 请求更多 schema。
   - Reward：`+1` 执行成功且 EX 匹配；`-1` 失败； shaping：减少无效重试。
   - 训练：**离线** 用轨迹做 RL / BC；或 **在线 bandit** 选策略（实现难度大，简历可写「原型」）。

2. **Stream-SQL / Flink SQL（与流处理背景契合）**
   - 限定问题类型：`窗口`、`Kafka topic`、`实时聚合`；单独 Router 分流。
   - Prompt 与方言模板换成 Flink SQL；Executor 用容器化 Flink 或本地 MiniCluster（重，可作为「规划 + 静态校验」弱化执行）。

3. **EXPLAIN / 代价感知**
   - 执行前 `EXPLAIN QUERY PLAN`（SQLite）或 `EXPLAIN (FORMAT JSON)`（PG）。
   - 在多候选 **语义等价难以判定** 时，选 **估算代价更低** 的一条；或在 Refiner 中要求「降低全表扫描」。

#### B.8 完整版评测与实验矩阵（建议写进论文 / README）

**数据集**：BIRD dev（或分层抽样）；可选 Spider dev 作辅助泛化。

**必跑消融（行 = 配置）**：

| Run ID | Schema | Decomposer | Multi-cand | Correction | 备注 |
|--------|--------|------------|------------|------------|------|
| R0 | Full/heuristic truncate | off | off | off | Baseline |
| R1 | Hybrid RAG | off | off | off | 验 RAG |
| R2 | Hybrid RAG | on/off | off | off | 验拆解 |
| R3 | Hybrid RAG | 最优固定 | on | off | 验投票 |
| R4 | Hybrid RAG | 最优固定 | on | on | Full |
| R5 | + 列级 linking | … | … | … | 分量 |
| R6 | + FK expand | … | … | … | 分量 |

**指标**：

- **EX（Execution Accuracy）**：主指标。
- **VES**：若官方脚本可跑（BIRD）。
- **Efficiency**：平均延迟、P95、LLM token、$/千题。
- **Robustness**：按错误类型统计（syntax / wrong column / wrong join / empty）。

**分层报告**：按数据库大小桶（列数 / 表数）、按问题难度（若数据提供 difficulty）。

#### B.9 工程化（完整版交付标准）

1. **配置**：`pydantic-settings` 或 YAML：模型名、temperature、K_table、K_col、`max_retry`、超时。
2. **Docker Compose**：`app` +（可选）`postgres` / 挂载 `bird/Databases`）；**勿**把 API Key 打进镜像。
3. **可观测性**
   - **LangSmith**：TRACE 每条样本；tag run_id。
   - 或 **OpenTelemetry**：span = node 名；metrics = 成功率、延迟。
4. **日志**：结构化 JSON（`trace_id`、`db_id`、`question_id`）。
5. **CI**：push 时跑 `pytest` + `eval smoke`（固定 10 题种子）。

#### B.10 完整版推荐目录结构（在 Mini 结构上增量）

```text
src/
├── agent/
│   ├── graphs/
│   │   ├── mini_graph.py
│   │   └── full_graph.py
│   ├── nodes/
│   │   ├── router.py
│   │   ├── schema_retriever.py
│   │   ├── decomposer.py
│   │   ├── fewshot_selector.py
│   │   ├── generator.py
│   │   ├── executor.py
│   │   ├── voter.py
│   │   └── refiner.py
│   └── state.py
├── retrieval/
│   ├── hybrid.py          # BM25 + dense + RRF
│   ├── fk_expand.py
│   └── column_prune.py
├── guardrails/
│   └── sql_ast.py         # sqlglot
├── observability/
│   └── tracing.py
└── eval/
    ├── protocols.py       # 统一输入输出
    ├── bird_runner.py
    └── report.py          # 消融表 Markdown 导出
configs/
└── full.yaml
```

#### B.11 里程碑（按周参考，可按课程表压缩）

| 周次 | 交付 |
|------|------|
| W1 | Mini 闭环合并进仓库；统一 State；评测脚本可跑通 R0/R4 雏形 |
| W2 | Hybrid Schema + FK expand；列级 pruning v1；消融 R1/R5 |
| W3 | Decomposer + 子执行（CTE 或逐步）；Router v1 |
| W4 | 多候选生成 + 执行投票；Refiner 与「检索扩大」联动 |
| W5 | AST guardrail、超时与资源限制、结构化日志 |
| W6 | Docker Compose + LangSmith；完整消融矩阵；README 图表 |
| W7～W8 | 可选差异化一条（B.7）+ 博客 / 演示视频；Spider 抽查泛化 |

#### B.12 完整版 Definition of Done（验收清单）

- [ ] `full_graph` 可在命令行对单条 / 批量样本运行，且 **配置可调**。  
- [ ] README 含 **架构图** + **消融表**（至少 R0/R1/R4/R5 四行真实数字）。  
- [ ] 任意一次运行可关联 **trace**（LangSmith 或导出 JSONL spans）。  
- [ ] Docker 一键拉起 Demo（或文档三步内可复制）。  
- [ ] 错误类型统计附录（可选一页 notebook）。  
- [ ] （若选 B.7）独立小节描述假设、方法、局限与复现实验命令。

---

## 6. 仓库结构建议

```text
nl2sql-agent/
├── README.md
├── requirements.txt
├── .env.example
├── docker-compose.yml          # 可选
├── src/
│   ├── agent/
│   │   ├── graph.py           # LangGraph 编排入口
│   │   ├── nodes.py           # 各节点实现
│   │   └── prompts.py         # Prompt 模板
│   ├── retriever/
│   │   └── schema_rag.py
│   ├── executor/
│   │   └── sandbox.py
│   └── eval/
│       └── run_eval.py
├── app.py                     # Streamlit / CLI，可选
├── data/                      # 数据集路径（勿提交大文件时用 gitignore）
└── results/                   # 评测输出
```

完整版推荐目录与拆分方式见 **§5-B.10**。

---

## 7. 交付物清单

- [ ] 可运行流水线：`question → （RAG）→ SQL → 沙箱执行 → （纠错循环）`
- [ ] 评测脚本与 README 中的对比表格（真实数字）
- [ ] `.env.example`（模型名、API Key 占位）
- [ ] 架构图（Excalidraw / draw.io 导出）
- [ ] Demo（GIF 或 Streamlit）
- [ ] （可选）技术博客链接、Docker、LangSmith trace

---

## 8. 非目标（避免 scope creep）

- 第一期不要求覆盖所有方言与权限模型；先 SQLite / DuckDB / 单 PG。
- 不要求一上来就 SOTA 榜单；要求 **可复现数字 + 清晰消融**。
- 生产级安全（SQL 注入审计、行级权限）可二期再做；Mini 期至少 **只读 + 超时**。

---

## 9. 下一个 Agent 的任务优先级

**Mini 路径**：  
1. 初始化仓库与依赖；跑通 **question → SQL → 沙箱执行** 的最短路径。  
2. 接入 **Schema RAG** 与 baseline 对比 token / EX。  
3. 接入 **Self-Correction** 循环与 `max_retry`。  
4. 固化评测脚本与 README 表格；补 Demo。

**完整版路径（接 Mini 之后）**：严格按 **§5-B.11 周里程碑** 推进；模块优先级与 **§5-B.1～B.6** 对齐——先 Hybrid Schema + FK 扩展与列级 pruning，再 Decomposer，再多候选投票与 Refiner 联动，最后观测与 Docker；**§5-B.7** 任选一条在 W7～W8 集中实现。

---

## 10. 项目约束（请填写后交给执行方）

| 项 | 填写 |
|----|------|
| Python 版本 | 例如 3.11+ |
| LLM / 预算 | 例如 DeepSeek / OpenAI |
| 数据集 | 例如 BIRD 子集 N 题 |
| 周期 | Mini ___ 天 / 完整版 ___ 周 |
| 执行后端 | 例如 DuckDB attach SQLite |

---

## 11. 参考方向（开源 / 论文，按需深入）

- **开源**：MAC-SQL（多 Agent）、CHESS（schema pruning）、XiYan-SQL、Vanna.AI、ora（LangGraph 示例）、DAIL-SQL。
- **数据集**：BIRD、Spider、Spider 2.0。
- **榜单**：<https://bird-bench.github.io/>

---

*文档版本：已扩展 §5-B 完整版详细规格；与仓库同步维护时可更新「约束」表格与交付勾选。*
