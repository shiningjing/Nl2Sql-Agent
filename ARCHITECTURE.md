# NL2SQL Mini Agent — 架构与核心逻辑

## 整体流程

```
                        用户问题 (NL)
                            │
              ┌─────────────┼─────────────┐
              │             │             │
         rag_schema    rag_domain    (开关控制)
              │             │
              ▼             ▼
    ┌─────────────────────────────┐
    │        RAG 检索              │
    │                              │
    │  question → Embedding向量    │
    │       ↓                      │
    │  ChromaDB top-k 相似度搜索    │
    │       ↓                      │
    │  ┌──────────┬─────────────┐  │
    │  │ schema块  │  domain块    │  │
    │  └──────────┴─────────────┘  │
    └─────────────┬───────────────┘
                  │
      ┌───────────┴───────────┐
      │   拼装 Prompt          │
      │                        │
      │  ## SCHEMA             │
      │    RAG选的表DDL         │
      │    + 表名目录(兜底)      │
      │  ## SAMPLE ROWS        │
      │    每表3行样本           │
      │  ## RETRIEVED NOTES    │
      │    RAG检索的业务知识     │
      │  ## USER QUESTION       │
      └───────────┬────────────┘
                  │
                  ▼
    ┌─────────────────────────────┐
    │     Generator Agent          │
    │                              │
    │  System: 规则+示例+方言        │
    │  User:   Prompt(拼装结果)     │
    │     ↓                        │
    │  DeepSeek API (temperature=0) │
    │     ↓                        │
    │  extract_sql() 三层防御提取    │
    │  validate_sql() 安全校验      │
    └─────────────┬───────────────┘
                  │
                  ▼  SQL
    ┌─────────────────────────────┐
    │     Reviewer Agent    ← M3  │
    │                              │
    │  ┌─ 硬校验(代码级) ──────────┐│
    │  │ 提取SQL中所有标识符        ││
    │  │ vs Schema 列名/表名集合    ││
    │  │ 未知标识符 → hallucination ││
    │  └──────────────────────────┘│
    │     ↓                        │
    │  ┌─ LLM审查(语义级) ────────┐│
    │  │ 5项检查清单               ││
    │  │ → JSON {valid, issues}   ││
    │  └──────────────────────────┘│
    │     ↓                        │
    │  合并 → {valid, issues}       │
    └─────────────┬───────────────┘
                  │
        ┌──── valid? ────┐
        │                 │
       true             false
        │                 │
        ▼                 ▼
     返回 SQL      ┌──────────────────┐
                   │ 回灌 Generator     │
                   │ last_error: issues │
                   │ last_sql: 旧SQL    │
                   │ 最多2轮            │
                   └──────┬───────────┘
                          │
                    Generator 修订
                          │
                    Reviewer 再审 ──→ 2轮后放行
                          │
                          ▼
                       最终 SQL
```

---

## 各模块核心伪代码

### schema.py

```
def get_schema_summary():
    engine = connect(demo.db)
    inspector = inspect(engine)

    for each table:
        columns[] = inspector.get_columns(table)
        pk        = inspector.get_pk_constraint()
        fks       = inspector.get_foreign_keys()

        members = []
        for col in columns:
            members += "  col TYPE [NOT NULL] [PRIMARY KEY]"
        if fks exist:
            for fk in fks:
                members += "  FOREIGN KEY (src) REFERENCES tgt(cols)"

        output += "CREATE TABLE table (\n"
               + join(members, ",\n")
               + "\n);"

    return output   // 纯文本DDL块
```

### generate.py

```
SYSTEM_PROMPT = """
  你是SQL生成助手。
  Rules: 方言={dialect}, 只允许SELECT,
         只用SCHEMA中的标识符, 只输出```sql块,
         默认LIMIT 200
  Example: Q→SQL静态示例
"""

def generate_sql(args):
    chat = ChatOpenAI(model, base_url, temperature=0)

    user_message = join([
        "## SCHEMA\n"        + schema_text,
        "## SAMPLE ROWS\n"   + sample_rows,
        "## RETRIEVED NOTES\n" + rag_context,
        "## USER QUESTION\n" + question,
        "## LAST_ERROR\n"    + last_error,    // retry时有
        "## LAST_SQL\n"      + last_sql,      // retry时有
    ])

    response = chat.invoke(System + Human)
    sql = extract_sql(response)    // 三层防御
    ok  = validate_sql(sql)        // 安全检查
    return sql, raw

def extract_sql(response):
    // Layer1: 正则 ```sql ... ```
    // Layer2: 正则 SELECT ... ;
    // Layer3: 返回原文 → 校验层会拦截
```

### rag_ingest.py + rag_retrieve.py

```
// ══ 入库 ══
def load_corpus(corpus_dir):
    chunks = []

    // 1) 自动: 每表一个schema块
    for table in tables:
        chunks += {
            content: DDL + samples + "Related: → other_table",
            metadata: {chunk_type: "schema", table_name: table}
        }

    // 2) 人手: 按 ##/### 标题切markdown
    for .md in corpus/:
        for section in split_by_headers(text):
            chunks += {
                content: section,
                metadata: {chunk_type: "domain", heading: title}
            }
    return chunks

def ingest_chunks(chunks, reset):
    client = ChromaDB.PersistentClient(.chroma)
    if reset: client.delete_collection()
    collection = client.get_or_create(
        "nl2sql_corpus",
        embedding_function = SentenceTransformer(BGE-small-zh)
    )
    collection.add(ids, documents, metadatas)

// ══ 检索 ══
def retrieve(question, k=8):
    collection = get_collection()
    results = collection.query(question, n_results=k)
    return [{content, source, metadata}]

// ══ 分流 ══
def build_prompt_context(chunks):
    schema = join([c.content for c in chunks if c.type=="schema"])
    notes  = join([c.content for c in chunks if c.type=="domain"])
    return {schema_text, notes_text}
```

### review.py (M3)

```
REVIEWER_PROMPT = """
  你是SQL审查专家, 只挑错不生成。
  检查: Schema对齐 / 语法 / 业务口径 / 方言 / 安全
  输出: 严格JSON {valid, issues[], suggested_fix}
"""

def review(sql, schema_text, notes_text):
    // ── 硬校验: 代码级, 100%可靠 ──
    schema_tables, schema_cols = parse(schema_text)
    hard_issues = []

    for "table.col" in sql:          // 检查限定列名
        if "table.col" not in schema_cols:
            hard_issues += hallucination

    for bare_word in sql:            // 检查裸列名
        if bare_word in keywords: continue
        if bare_word in schema_cols: continue
        if bare_word in qualified_parts: continue
        hard_issues += hallucination

    // ── LLM审查: 语义级, 覆盖面广 ──
    chat = ChatOpenAI(temperature=0)
    user_msg = join([schema, notes, hard_issues, "```sql\n"+sql+"\n```"])
    response = chat.invoke()
    result = extract_json(response)   // 三层防御解析

    // ── 合并 ──
    result.issues = hard_issues + result.issues
    if hard_issues: result.valid = False
    return result
```

### pipeline.py (主编排)

```
MAX_REVIEW_ROUNDS = 2

def run(question, rag_schema, rag_domain, reviewer_on, k=8):
    // Step 1: RAG
    chunks = retrieve(question, k)
    {schema_text, notes_text} = build_prompt_context(chunks)
    if not schema_text: schema_text = full_ddl()  // 兜底

    // Step 2: Generator
    sql = generate_sql(schema, question, notes, samples)

    // Step 3: Reviewer循环
    review_rounds = []
    if reviewer_on and sql:
        for round in [0, 1]:
            result = review(sql, schema, notes)
            review_rounds += result
            if result.valid: break

            // 回灌修订
            feedback = format(result.issues + result.suggested_fix)
            sql = generate_sql(..., last_error=feedback, last_sql=sql)
            if not sql: break

    return {sql, review_rounds, ...}
```

---

## 开关矩阵

```
run(question,
    rag_schema  = True,   // Schema Selection RAG
    rag_domain  = True,   // Domain Knowledge RAG
    reviewer_on = True,   // Reviewer Agent (M3)
)
```

| rag_schema | rag_domain | reviewer_on | 效果 |
|:----------:|:----------:|:-----------:|------|
| T | T | T | 全开（默认） |
| T | T | F | M1+M2，跳过审查 |
| F | T | T | 全量DDL + 业务RAG + 审查 |
| F | F | F | 纯M1裸跑 |
