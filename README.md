<p align="right"><a href="README_zh.md">中文</a></p>

# NL2SQL Agent

Natural language to SQL based on LangGraph with RAG-augmented schema retrieval and self-correcting execution loops.

## Architecture

```
                         ┌─────────────────────┐
                         │   Schema Retriever  │
                         │   RAG + DDL build   │
                         └──────────┬──────────┘
                                    │
                         ┌──────────▼──────────┐
                         │       Router        │
                         │   simple / complex  │
                         └──────────┬──────────┘
                                    │
                         complex ───┴─── simple
                           │                │
                ┌──────────▼──────────┐     │
                │     Decomposer      │     │
                │   sub-question DAG  │     │
                └──────────┬──────────┘     │
                           │                │
                           └──────┬─────────┘
                                  │
                       ┌──────────▼──────────┐
                       │   Few-shot Selector │
                       │   Top-K example     │
                       └──────────┬──────────┘
                                  │
                       ┌──────────▼──────────┐
                       │      Generator      │
                       │  multi-candidate    │
                       │  (temp 0/0.3/0.6)  │
                       └──────────┬──────────┘
                                  │
                       ┌──────────▼──────────┐
                       │        Guard        │
                       │  syntax + hallucin. │
                       └──────────┬──────────┘
                                  │
                        pass ─────┴───── fail
                          │                │
               ┌──────────▼──────────┐  ┌──▼──────────┐
               │        Voter        │  │   Refiner   │
               │  parallel exec +    │  │  error →    │
               │  LLM fallback       │  │  Generator  │
               └──────────┬──────────┘  └─────────────┘
                          │
                  winner ─┴── no winner → Refiner
                          │
               ┌──────────▼──────────┐
               │   Semantic Check    │
               │   LLM YES / NO      │
               └──────────┬──────────┘
                          │
                   YES ───┴─── NO → Refiner
                          │
                         END
```

## Quick Start

```bash
pip install -r requirements.txt
cp .env.example .env        # add your LLM_API_KEY
python scripts/ingest.py    # build RAG index
streamlit run app.py        # start frontend (http://127.0.0.1:8501)
```

Or with Docker (includes MySQL + PostgreSQL demo databases):

```bash
docker compose up -d                 # start all services
python scripts/_smoke_multidb.py     # multi-database smoke test (9 questions × 3 dialects)
```

## LLM Provider Switching

Streamlit sidebar Provider dropdown with 4 presets, auto-fills model name and API URL:

| Provider | Model | Notes |
|----------|-------|-------|
| DeepSeek V4 Pro | deepseek-v4-pro | Default, OpenAI-compatible |
| OpenAI GPT-4o | gpt-4o | Requires OpenAI API key |
| Claude Opus 4.7 | claude-opus-4-7 | Auto-switches to ChatAnthropic |
| Custom | (any) | OpenAI-compatible APIs |

Keys stored in `llm_keys.json` (git-ignored):
```json
{"deepseek": "sk-xxx", "openai": "sk-xxx", "anthropic": "sk-ant-xxx"}
```

## Bring Your Own Database

Edit `databases.json` — dialect, table count, and online status auto-detected:

```json
{
  "databases": [
    {"db_id": "my_mysql", "display_name": "Production MySQL",
     "database_url": "mysql+pymysql://user:pass@host:3306/db"},
    {"db_id": "my_pg", "display_name": "Analytics PG",
     "database_url": "postgresql+psycopg2://user:pass@host:5432/db"},
    {"db_id": "docker_mysql", "display_name": "Docker MySQL Demo",
     "database_url": "mysql+pymysql://nl2sql:nl2sql@127.0.0.1:3306/demo"},
    {"db_id": "docker_pg", "display_name": "Docker PG Demo",
     "database_url": "postgresql+psycopg2://nl2sql:nl2sql@127.0.0.1:5432/demo"}
  ]
}
```

Docker users: `docker compose up -d mysql postgres` to spin up demo databases.

Save and refresh Streamlit — databases appear in the dropdown. Offline databases are tagged "(offline)".

## API

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/query` | Mini pipeline (single-gen + self-correction) |
| `POST` | `/api/v1/query/full` | Full LangGraph pipeline (Vote + SemCheck) |
| `POST` | `/api/v1/query/full/stream` | Full pipeline SSE streaming (per-node progress) |
| `GET` | `/api/v1/health` | Health check |
| `GET` | `/api/v1/schema` | Schema DDL + table catalog |

```json
// POST /api/v1/query/full
{
  "question": "Find the top 5 products by revenue",
  "db_id": "mysql_demo",
  "database_url": "mysql+pymysql://nl2sql:nl2sql@127.0.0.1:3306/demo",
  "llm": {"model": "deepseek-v4-pro", "api_key": "sk-xxx", "base_url": ""}
}
```

## Multi-Database

| Dialect | Connection String | Source |
|---------|------------------|--------|
| SQLite | `sqlite:///./data/demo.db` | Built-in BIRD (11 DBs) |
| MySQL | `mysql+pymysql://user:pass@localhost:3306/demo` | User JSON |
| PostgreSQL | `postgresql+psycopg2://user:pass@host:5432/demo` | User JSON |

Dialect auto-detected from connection string — injects dialect-specific few-shot (`corpus/bird_fewshot/mysql.md`, `postgresql.md`), switches AST validation dialect, and applies corresponding error classification rules.

## BIRD Mini-Dev Results

500 samples, 11 databases. Full LangGraph pipeline, module-by-module ablation.

| Config | EX | VES | Time | Description |
|--------|-----|-----|------|-------------|
| R0_Baseline | 23.4% | 0.334 | 6.98s | Generator only |
| R1_Decomposer | 23.8% | 0.374 | 5.80s | + decomposition |
| R2_RAG | **34.6%** | **0.506** | 5.06s | + RAG (best ROI) |
| R3_MultiCandidate | 34.0% | 0.376 | 9.45s | + multi-candidate voting |
| R4_PruneFewshot | 37.4% | 0.353 | 10.75s | + column pruning + few-shot |
| R5_Evidence | **38.8%** | 0.303 | 12.88s | + BIRD human evidence |

### By Difficulty

| Config | Simple (148) | Moderate (250) | Challenging (102) |
|--------|:------------:|:--------------:|:-----------------:|
| R0_Baseline | 37.8% | 20.8% | 8.8% |
| R2_RAG | 48.6% | 30.8% | 23.5% |
| R4_PruneFewshot | 52.0% | 36.0% | 19.6% |
| R5_Evidence | 49.3% | 38.0% | 25.5% |

## Evaluation

```bash
# Quick test (20 samples)
python scripts/eval_bird.py --test --samples 20 --configs R2,R5

# Full ablation matrix (500 samples, with checkpoint resume)
python scripts/eval_bird.py --exp ablation --max-workers 8

# Pre-compute gold SQL cache (one-time)
python scripts/_precompute_gold.py
```

## Tech Stack

| Layer | Choice |
|-------|--------|
| LLM | DeepSeek / OpenAI / Claude |
| Embedding | BAAI/bge-small-zh-v1.5 (local) |
| Vector DB | ChromaDB |
| Orchestration | LangGraph + LangChain |
| Databases | SQLite / PostgreSQL / MySQL |
| AST Guard | sqlglot (auto-detect dialect) |
| API | FastAPI + Pydantic |
| Cache | Redis |
| Frontend | Streamlit |

## Environment

| Variable | Default |
|----------|---------|
| `LLM_BASE_URL` | `https://api.deepseek.com/v1` |
| `LLM_CHAT_MODEL` | `deepseek-v4-pro` |
| `LLM_API_KEY` | (from `llm_keys.json` or env) |
| `DATABASE_URL` | `sqlite:///./data/demo.db` |
| `REDIS_URL` | `redis://localhost:6379/0` |
| `EMBED_MODEL_NAME` | `BAAI/bge-small-zh-v1.5` |
