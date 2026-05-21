# NL2SQL Mini Agent（RAG + 双 Agent 协作 + 执行自修复）— 实现移交说明

> **用途**：把本文档整体提供给下一个 AI Agent / 协作者，按「目标 → 架构 → 里程碑 → 接口约定」完成实现。  
> **用户背景**：研二，主线为 Flink/AI4DB；本项目为 **1 周内可交付的个人工程**，用于简历「项目经历」。

---

## 1. 项目定位

### 1.1 一句话

在 **中小规模 SQLite（首选）或 DuckDB** 上，实现 **自然语言 → 可执行 SQL** 的 Mini Agent：**结构化 Schema 强约束** + **RAG 注入领域知识** + **Generator-Reviewer 双 Agent 协作** + **执行错误回灌自修复**，并具备 **可复现评测（RAG on/off）**。

### 1.2 明确不做（防范围爆炸）

- 不做「全互联网公开数据集」级别的 Text2SQL 竞赛刷榜（除非后期扩展）。
- 不做多租户生产级权限体系；默认 **单用户、本机、单库文件**。
- 不做复杂的分布式查询、跨库联邦；**单库多表** 即可。
- 默认 **只读**：生成 SQL **仅允许 `SELECT`**（或 `WITH ... SELECT`），禁止 `INSERT/UPDATE/DELETE/DROP`。

---

## 2. 技术栈（定稿）

| 层级    | 选型                                                   | 备注                                                                |
| ----- | ---------------------------------------------------- | ----------------------------------------------------------------- |
| 语言    | Python **3.10+**                                     |                                                                   |
| 向量库   | **ChromaDB**（本地持久化目录）                                | 优先 `chromadb` + 固定 persist path                                   |
| 编排    | **LangChain**（或 **LlamaIndex**，二选一，**默认 LangChain**） | 实现时选你更熟的；移交以 LangChain 描述为准                                       |
| DB 访问 | **SQLAlchemy** + **SQLite**（主）/ **DuckDB**（可选二选一）    | 用 `engine` 反射 metadata                                            |
| UI    | **Streamlit**                                        | 快速演示：问题输入、展示检索片段、SQL、结果表、错误信息                                     |
| LLM   | **OpenAI 兼容 API**（Chat + Embeddings）或 **国内大模型 API**  | 用 `.env` 配置 `BASE_URL` / `API_KEY` / `CHAT_MODEL` / `EMBED_MODEL` |

### 2.1 环境变量（建议）

```env
LLM_BASE_URL=          # 可选，默认可用官方或代理
LLM_API_KEY=
LLM_CHAT_MODEL=        # 如 gpt-4o-mini / qwen-turbo 等
LLM_EMBED_MODEL=       # 如 text-embedding-3-small；国内模型按文档填写
SQL_DIALECT=sqlite     # sqlite | duckdb
DATABASE_URL=sqlite:///./data/demo.db
CHROMA_PERSIST_DIR=./.chroma
CORPUS_DIR=./corpus
```

---

## 3. 系统架构（逻辑）

```
用户问题（NL）
    │
    ├─► [A] 结构化 Schema 摘要（强约束）
    │       - SQLAlchemy inspect：表名、列名、类型、PK/FK（压缩成 prompt 块）
    │
    ├─► [B] RAG 检索（增强）
    │       - 语料：corpus/*.md（业务释义、口径、同义词、Few-shot Q→SQL）
    │       - Embedding → Chroma top-k（k 建议 4~8，可配置）
    │
    ├─► [C] Generator Agent — 生成 SQL
    │       - 系统提示词：专注「翻译」，从 Schema + RAG 上下文到 SQL
    │       - 输出原始 SQL 候选
    │
    ├─► [D] Reviewer Agent — 静态审查 SQL（新增：双 Agent 核心）
    │       - 系统提示词：专注「挑错」，只审查不生成
    │       - 交叉比对 Schema：检测幻觉列名/表名
    │       - 检查 JOIN 缺 ON、GROUP BY 缺聚合函数、业务口径一致性
    │       - 输出：{ "valid": true/false, "issues": [...], "suggested_fix": "..." }
    │
    ├─► 若 Reviewer 发现硬伤 → 返回 Generator 修订（不消耗执行轮数，最多 2 轮）
    ├─► 若 Reviewer 通过 → 进入执行
    │
    └─► [E] 执行 + 错误回灌自修复
            - 若执行失败 → 将 DB error message 回灌 Generator → 最多 **2** 轮修订
            - 默认给结果 SQL 自动加 **LIMIT 200**（除非用户明确要全部，仍可上限封顶）
            - 2 轮后仍失败 → 友好降级提示
```

### 3.0 两层质量保障（双 Agent 的设计动机）

```
第一层：Reviewer Agent（静态分析，执行前）
  └─ 拦截幻觉列名、语法硬伤、业务口径偏离
  └─ 来源：Schema 交叉比对 + RAG 口径校验

第二层：DB 错误回灌（动态反馈，执行后）
  └─ 捕获 SQLite 运行时错误，回灌 Generator 修复
  └─ 来源：DB engine 原生报错
```

这种分层设计让每个 Agent 有不可替代的分工：Generator 只管"翻译"，Reviewer 只管"挑错"，二者视角天然互补。

### 3.1 关键设计原则

1. **Schema 必须完整进入上下文**（或可验证的子集），不能只靠 RAG「猜列名」。  
2. **RAG 只补充**「业务怎么说、字段含义、口径、样例」，不承担枚举全列职责。  
3. **双 Agent 分工不可替代**：Generator 专注"翻译"，Reviewer 专注"挑错"——两个 system prompt 视角天然互补，而非为多而多。Reviewer 在 SQL 执行前拦截错误，减少无效执行和 API 调用。  
4. **自修复闭环**是质量核心：比单纯「一次生成」更接近可演示的工程品。  
5. **自我修复兜底**：Generator→Reviewer 修订和 DB 错误回灌修复各有最多 2 轮；若任一路径耗尽仍失败，返回友好降级提示（如"无法生成有效 SQL，请尝试换一种问法或检查数据库 Schema"），并在 UI 上展示原始 SQL → Reviewer 反馈 → 修订 SQL → 执行的完整演进过程，作为 Demo 亮点。  
6. **评测**必须有 **RAG 开/关** 对照，否则简历数字站不住。

---

## 4. 语料与索引（RAG）

### 4.1 目录建议

```
corpus/
  00_glossary.md          # 业务名词 ↔ 字段/表
  01_schema_notes.md      # 表级说明（每张表一小节）
  02_rules.md             # 指标口径：如「有效订单=…」
  fewshot/
    01.md                 # ### Q ... ### SQL ...（多条）
    02.md
data/
  demo.db                 # SQLite 示例库
  seed.sql                # 建表+灌测试数据（可重复执行）
scripts/
  ingest.py               # 读取 corpus → 切分 → 写入 Chroma
```

### 4.2 切分策略（实现要求）

- Markdown 按 `##` / `###` 标题切 chunk；每 chunk **200~600 字**为宜。  
- 写入 Chroma 时 `metadata` 至少包含：`source_path`、`heading`、`chunk_id`。  
- **ingest** 支持 `--reset` 清空集合重建。

### 4.3 Few-shot 格式（推荐）

```markdown
### Q
过去7天每个品类的订单总额是多少？

### SQL
SELECT c.name AS category, SUM(o.amount) AS gmv
FROM orders o
JOIN products p ON o.product_id = p.id
JOIN categories c ON p.category_id = c.id
WHERE o.created_at >= date('now', '-7 days')
GROUP BY c.name
ORDER BY gmv DESC
LIMIT 200;
```

---

## 5. LLM Prompt 合同（实现时必须固化）

### 5.1 系统提示词应包含（要点清单）

- 方言：`SQL dialect = {sqlite|duckdb}`，仅使用该方言函数与语法。  
- **只允许 `SELECT`**（含 `WITH`）。  
- **只能使用**「提供的表/列清单」中的标识符；禁止臆造列。  
- 输出格式：**仅输出一个 SQL 代码块**（便于解析）。  
- **默认 LIMIT**：若用户未要求全量，追加 `LIMIT 200`。  
- 若上一轮执行报错：根据 **DB 错误信息** 修订 SQL，仍遵守上述约束。

### 5.3 SQL 输出解析策略（防御性提取）

LLM 的输出不可靠——它经常在代码块外加解释文字，或使用非标准格式。**不能假设 LLM 一定遵守输出格式约束**，必须做防御性提取：

1. 首选：正则提取 ` ```sql ... ``` ` 代码块 → 取第一个匹配。
2. 若无代码块：正则匹配 `SELECT`（或 `WITH`）开头的完整语句到行尾 → 截取到分号或空行。
3. 若以上均失败：将 LLM 的原始回复 + 提示"上次未按格式输出，请仅输出一个 SQL 代码块"作为 `LAST_ERROR` 上下文回灌，触发下一轮生成（计入修复轮数）。
4. 提取出的 SQL 做轻量校验：
   - 必须包含 `SELECT` 关键字
   - 不包含禁止关键字（见第 6 节）
   - 若有多条语句（`;` 分隔），仅取第一条

### 5.4 用户消息拼装顺序（建议）

1. `## SCHEMA`（压缩 DDL + PK/FK）  
2. `## RETRIEVED NOTES`（RAG 片段，标注 source）  
3. `## USER QUESTION`  
4. （若有）`## LAST_ERROR` + `## LAST_SQL`

### 5.5 Reviewer Agent 系统提示词（要点清单）

Reviewer 是独立 Agent，不生成 SQL，只做静态审查。其系统提示词与 Generator **完全分离**：

- **角色**：你是 SQL 审查专家，只负责审查他人生成的 SQL，你不生成新 SQL。

- **审查项**：
  
  1. **Schema 对齐**：SQL 中每个表名、列名是否存在于下方 Schema 清单中。若发现不在清单中的标识符 → issue。
  2. **语法完备性**：JOIN 是否缺 ON 条件、GROUP BY 是否缺聚合函数、子查询是否有别名。
  3. **业务口径**：SQL 逻辑是否与 RETRIEVED NOTES 中的业务规则一致（如"有效订单需 status=1"、"GMV 不含退款"）。
  4. **方言合规**：函数和语法是否符合 SQLite/DuckDB 方言。
  5. **安全检查**：是否包含 INSERT/UPDATE/DELETE/DROP 等禁止关键字。

- **输出格式（严格 JSON）**：
  
  ```json
  {
    "valid": true,
    "issues": [],
    "critical_count": 0
  }
  ```
  
  或：
  
  ```json
  {
    "valid": false,
    "issues": [
      {"type": "hallucination", "detail": "Column 'order_date' not in schema. Did you mean 'created_at'?"},
      {"type": "business_rule", "detail": "Query does not filter by status=1 as required by RETRIEVED NOTE #2."}
    ],
    "critical_count": 2,
    "suggested_fix": "Replace 'order_date' with 'created_at'; add WHERE status=1."
  }
  ```

- **reviewer_mode**：`strict`（默认）——有 hallucination 类 issue 直接判 invalid；`lenient`——仅警告，仍放行。

- 若 `valid=false`，Generator 根据 `issues` 和 `suggested_fix` 修订 SQL，不计入执行修复轮数。Generator→Reviewer 修订最多 2 轮，之后即使 invalid 也放行执行。

### 5.6 Reviewer JSON 输出解析策略

Reviewer 同样不可靠——它可能在 JSON 外加 Markdown 注释，或 JSON 格式错误：

1. 首选：正则提取 ` ```json ... ``` ` 代码块 → `json.loads()`
2. 若无代码块：正则匹配 `{ ... }` 最外层 JSON 对象 → `json.loads()`
3. 若 JSON 解析失败：将 Reviewer 原始输出全文作为 `issues` 文本注入 Generator 的 `LAST_ERROR`，触发修订
4. 解析成功但缺少必需字段 → 用默认值补全（`valid=false`, `issues=["Reviewer output unparseable"]`）

---

## 6. SQL 执行与安全

- 使用 **SQLAlchemy text()** 执行；**参数化**仅用于预编译检查（可选）。  
- 执行前可做轻量校验：  
  - 正则/解析器禁止 `;` 多条语句（防注入式多语句）  
  - 禁止关键字：`INSERT` `UPDATE` `DELETE` `DROP` `ALTER` `CREATE` `PRAGMA`（可按需收紧）  
- **超时**：查询超时（如 5s）可选，演示阶段可简化。

---

## 7. 评测与报表（交付硬指标）

### 7.1 构造测试集 `eval/gold.jsonl`

每行 JSON：

```json
{
  "id": "e001",
  "question": "……",
  "gold_sql": "SELECT …",
  "database": "data/demo.db"
}
```

建议开发阶段 **N=10**，全系统跑通后补至 **N=20**（降低统计波动，简历数字更可信）。覆盖：

- 单表过滤/聚合  
- 两表 **JOIN**  
- 时间范围（`date('now', '-7 days')` 类）  
- `GROUP BY` + `HAVING`  
- 子查询（可选 1 题）

### 7.2 指标（实现脚本 `scripts/eval.py`）

对每条样例运行：

- **可执行率**：生成 SQL 能跑通且无异常  
- **结果等价性（弱）**（一周内可选）：  
  - 对 gold 与 pred 都做 `EXPLAIN QUERY PLAN` 对比（仅参考）  
  - 或对结果集排序后比较 hash（行数上限内）  
- **RAG on/off**：同一套题，开关 RAG，对比可执行率或弱等价率 **提升 Δ**  

输出：`reports/eval-YYYY-MM-DD.md`（Markdown 表）

---

## 8. UI（Streamlit）页面要素

- 侧边栏：`k`、模型名、`RAG on/off`、`重置索引` 按钮（调用 ingest）  
- 主区：  
  - 输入框：自然语言问题  
  - 折叠区：展示 **检索到的 chunk**（标题+片段）  
  - 展示：**最终 SQL**、**执行耗时**、**结果表**（`st.dataframe`）  
  - 展示：**错误回灌轮数**、每轮错误摘要（调试友好）

---

## 9. 实现里程碑（建议顺序）

| 阶段  | 产出                                               | 验收                                                  |
| --- | ------------------------------------------------ | --------------------------------------------------- |
| M0  | `seed.sql` + `demo.db` + 10 条 gold               | 手工 SQL 能跑出结果                                        |
| M1  | Schema 反射 + Generator 单 Agent 生成（无执行，无 Reviewer） | 10 题中 ≥6 可解析出 SQL                                   |
| M2  | Chroma ingest + RAG 注入                           | 同一批题 RAG-on 可执行率 ≥ RAG-off                          |
| M3  | + Reviewer Agent 静态审查                            | Reviewer 拦截 ≥30% 的错误 SQL；Generator→Reviewer 协作闭环可演示 |
| M4  | + 执行 + 错误回灌 ≤2 轮（两层质量保障联动）                       | 语法错误类样本显著减少                                         |
| M5  | Streamlit Demo                                   | 3 分钟录屏可演示（含双 Agent 交互过程可视化）                         |
| M6  | `eval.py` 报告                                     | 产出 RAG Δ、Reviewer 拦截率、最终准确率，可写进简历                   |

---

## 10. 仓库结构（建议）

```
nl2sql-mini-agent/
  README.md
  requirements.txt
  .env.example
  app_streamlit.py
  nl2sql/
    schema.py
    rag_ingest.py
    rag_retrieve.py
    generate.py          # Generator Agent（单 Agent 生成）
    review.py            # Reviewer Agent（静态审查，新增）
    execute.py
    pipeline.py          # 编排 Generator → Reviewer → Execute → 自修复
  corpus/...
  data/...
  eval/...
  scripts/ingest.py
  scripts/eval.py
```

---

## 11. 依赖（`requirements.txt` 草稿）

```
streamlit>=1.32
sqlalchemy>=2.0
chromadb>=0.5
langchain>=0.2
langchain-openai>=0.1   # 或 langchain-community + 自定义 Chat/Embeddings
python-dotenv>=1.0
sqlparse>=0.5
```

（实现时按所选 LangChain 版本微调；若国内模型需换 `langchain_community` 的 Chat 封装。）

---

## 12. 已知风险与规避

| 风险                    | 规避                                                                   |
| --------------------- | -------------------------------------------------------------------- |
| Schema 太大挤爆 context   | 限制表数量（演示库 3~8 张表）；列过多时按表裁剪描述                                         |
| 模型胡编函数                | 系统提示词锁定方言；Reviewer 静态拦截 + 错误回灌修正                                     |
| Reviewer JSON 输出格式不稳定 | 防御性提取（见 5.6 节）：代码块提取 → 正则 fallback → 原文回灌                            |
| Reviewer 过度拦截（假阳性）    | 提供 `reviewer_mode=lenient` 开关；Reviewer 修订 2 轮后即使 invalid 也放行         |
| Embedding 费用          | 优先小维度模型；语料控制在百级 chunk                                                |
| 评测过松                  | 至少保留「可执行率 + RAG Δ + Reviewer 拦截率」；有时间再做结果 hash                       |
| 双 Agent 增加 API 调用成本   | Reviewer 仅在 Generator 输出后调用一次（非每轮都调）；用低成本模型做 Reviewer（如 gpt-4o-mini） |

---

## 13. 简历侧表述（实现完成后回填）

- **技术栈**：Python；LangChain；ChromaDB；SQLAlchemy；SQLite（或 DuckDB）；Streamlit；OpenAI API（或通义等）。  
- **亮点**：Generator-Reviewer 双 Agent 协作；RAG 语料分层；Schema 白名单；两层质量保障（静态审查 + 执行回灌自修复）；**N=?** 条金标；RAG on 可执行率提升 **Δ=?**；Reviewer 拦截率 **R=?%**；GitHub + Demo 视频。

---

## 14. 交接检查清单（下一个 Agent 开始前）

- [ ] 用户已确认：**SQLite 主题域**（电商/教务/图书…）与 **表清单**  
- [ ] 已确认：**LLM 供应商**（OpenAI 兼容 or 国内）与 **Embedding 模型名**  
- [ ] 目标交付：**仅 CLI** 还是 **必须 Streamlit**（默认必须）  
- [ ] 一周时间约束下：**N=10** 金标为最低线

---

**文档版本**：2026-05-06（与用户对话上下文一致）  
**维护**：实现过程中若架构有变，请更新「技术栈」「Prompt 合同」「评测指标」三节，避免与代码漂移。
