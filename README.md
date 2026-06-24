<p align="right"><a href="README_zh.md">中文</a></p>

# NL2SQL Agent

Natural language → SQL end-to-end system. LangGraph state machine orchestrates **Router → Schema Retriever → Decomposer → Generator → Guard → Voter → Semantic Check → Refiner** with RAG-augmented retrieval and self-correcting execution loops.

> **BIRD Mini-Dev** (100 samples): DeepSeek V4 Pro EX **39.0%** · RAG Table Recall **98.4%** · 200 calls **0 crashes** · Cost **$0.97** (¥7)

## Architecture

### System

```
  Client (Streamlit UI / API)
        │
        │  POST /task/submit    POST /task/{id}/feedback
        │  GET  /task/{id}/stream (SSE)
        ▼
  Go API Gateway (:8080)          rate-limit · health · reverse-proxy
        │
        ▼
  FastAPI  (:8000) ──────────────────────────────┐
        │                                         │
        │  submit ──→ Kafka ──→ Worker            │
        │  feedback ──→ Kafka ──→ Worker          │
        │                                         │
        │  SSE ←── Redis (poll state + Pub/Sub) ◄─┘
        │
        │  /query/full/stream: FastAPI runs LangGraph in-thread
        │  (sync path — no Kafka, no Worker)
        ▼
  ┌──────────────────────────────────────────────────────┐
  │  PostgreSQL · MySQL · SQLite (BIRD 11 DBs)           │
  │  ChromaDB (RAG embeddings) · Redis (state + tokens)  │
  └──────────────────────────────────────────────────────┘
```

### LangGraph Agent

```
                         ┌─────────────────────┐
                         │       Router        │
                         │   simple / complex  │
                         └──────────┬──────────┘
                                    │
                         ┌──────────▼──────────┐
                         │   Schema Retriever  │
                         │   RAG + DDL build   │
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
                       │  single (temp=0)    │
                       │  multi on retry     │
                       └──────────┬──────────┘
                                  │
                       ┌──────────▼──────────┐
                       │        Guard        │
                       │  AST + 9 rules      │
                       └──────────┬──────────┘
                                  │
                        pass ─────┴───── fail
                          │                │
               ┌──────────▼──────────┐  ┌──▼──────────┐
               │        Voter        │  │   Refiner   │◄── Human Feedback
               │  parallel exec +    │◄─│  error →    │   (POST /task/
               │  LLM tiebreak       │  │  Generator  │    {id}/feedback)
               └──────────┬──────────┘  └──▲──────────┘
                          │                │
                  winner ─┴── no winner ───┘
                          │
               ┌──────────▼──────────┐
               │   Semantic Check    │
               │   LLM YES / NO      │
               └──────────┬──────────┘
                          │
                   YES ───┴─── NO ───→ Refiner
                          │
                    ┌─────▼─────┐
                    │    END    │
                    └───────────┘
```

## Quick Start

### 1. Configure

```bash
cp .env.example .env
# Add your LLM keys to llm_keys.json:
# {"deepseek": "sk-xxx", "openai": "sk-xxx", "anthropic": "sk-ant-xxx"}
```

### 2. Start

```bash
docker compose -f deployment/docker-compose.yml up -d
```

This starts 6 services: **FastAPI** (:8000) + **Streamlit** (:8501) + **Redis** (:6379) + **PostgreSQL** (:5432) + **MySQL** (:3306) + **Kafka** (:9092).

To include Go services:

```bash
# Build and run Go MCP Server + Go API Gateway
cd tools/mcp-server-go && go build -o mcp-server-go.exe . && cd ../..
cd gateway && go build -o gateway.exe . && cd ..
```

### 3. Open

```bash
open http://localhost:8501    # Streamlit UI
open http://localhost:8000/docs  # FastAPI Swagger
```

### Quick Test

```bash
# Multi-database smoke test
python tests/smoke_multidb.py

# BIRD 20-sample sanity check
python -m evaluation.run --test --samples 20 --configs R2
```

## Features

| Feature | Description |
|---------|-------------|
| **LangGraph Pipeline** | 9-node state machine: Router → Schema → Decomposer → Fewshot → Generator → Guard → Voter → SemCheck → Refiner |
| **RAG Retrieval** | ChromaDB + BAAI/bge-small-zh-v1.5 embeddings. Table recall **98.4%** on BIRD |
| **SQL Safety (9 Rules)** | DDL rejection · multi-statement detection · LIMIT enforcement · statement timeout · sensitive column warning · table/column existence check · WHERE warning · subquery depth limit · JOIN table limit |
| **MCP Tools** | `validate_sql` (AST validation) + `execute_readonly_sql` (sandboxed execution). Python + Go dual implementations |
| **Self-Correction** | Guard/Voter/SemCheck failures → Refiner formats error → Generator retries. Fix rate **26.5%** |
| **Human Feedback** | Multi-turn natural language correction (up to 10 rounds). Full conversation history persisted |
| **SSE Streaming** | Real-time token streaming via Redis Pub/Sub + Server-Sent Events |
| **Async Tasks** | Kafka task submission → Worker consumption → Redis state machine (PENDING→RUNNING→SUCCESS/FAILED/TIMEOUT/CANCELLED) |
| **Go API Gateway** | go-chi HTTP router · sliding-window rate limit (100 req/min/IP) · health aggregation · structured logging |
| **Go MCP Server** | vitess/sqlparser AST validation · database/sql connection pool · dual check (regex + AST) · LIMIT auto-wrap · statement timeout |
| **Multi-Database** | SQLite / MySQL / PostgreSQL. Dialect auto-detection + dialect-specific few-shot |
| **Multi-Model** | DeepSeek V4 Pro / GPT-4o / Claude Opus 4.7 / Custom. Auto-detect Anthropic → ChatAnthropic |
| **Observability** | OpenTelemetry tracing · TraceLogger (jsonl) · per-node timing · token usage tracking |
| **Docker Compose** | 6 services one-command start. KRaft Kafka (no ZooKeeper). Pre-seeded demo databases |

## API (16 Endpoints)

### Task (Async)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/task/submit` | Submit NL2SQL task → Kafka → returns task_id (202) |
| `GET` | `/task/{id}/status` | Task status from Redis |
| `POST` | `/task/{id}/cancel` | Cancel running task |
| `GET` | `/task/{id}/stream` | SSE stream (token + node progress + result) |
| `POST` | `/task/{id}/feedback` | Human feedback for multi-turn correction |
| `GET` | `/task/{id}/health` | Task health + heartbeat |
| `POST` | `/task/scan-stale` | Scan stale/zombie tasks |

### Query (Sync)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/query` | Simple query (Generator + Guard + Executor) |
| `POST` | `/query/full` | Full pipeline (all 9 nodes) |
| `POST` | `/query/full/stream` | Full pipeline with SSE streaming |

### Eval

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/eval/start` | Start BIRD evaluation |
| `GET` | `/eval/status/{id}` | Evaluation task status |
| `GET` | `/eval/tasks` | List all eval tasks |
| `DELETE` | `/eval/cancel/{id}` | Cancel evaluation |

### Health

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Service health |
| `GET` | `/schema` | Database schema (DDL + catalog) |
| `GET` | `/databases` | List available databases |

## BIRD Mini-Dev Results

**Latest**: 100 samples, DeepSeek V4 Pro, Full Graph + RAG. 200 calls, 0 crashes.

| Metric | R2_RAG | R5_Evidence |
|--------|--------|-------------|
| **EX** | **39.0%** | **43.0%** |
| VES | 0.49 | 0.43 |
| Avg Time | 11.6s | 10.9s |
| Avg Tokens | 16,762 | 15,358 |
| RAG Table Recall | **98.4%** | 97.9% |
| Self-Correction Fix Rate | 26.5% | 24.1% |
| Cost (200 calls) | — | $0.97 (¥7) |

### By Difficulty

| Config | Simple (37) | Moderate (49) | Challenging (14) |
|--------|:-----------:|:-------------:|:----------------:|
| R2_RAG | 46.0% | 34.7% | 35.7% |
| R5_Evidence | 48.6% | 38.8% | 42.9% |

### Module Analysis (R2_RAG)

| Module | Metric | Value |
|--------|--------|-------|
| **Guard** | False Negative Rate | 55.6% (pure syntax, no semantic ability) |
| **SemCheck** | False Negative Rate | 38.9% (LLM YES but EX=0) |
| **Self-Correction** | Retry Rate / Fix Rate | 49.0% retried · 26.5% fixed |
| **Voter** | Single / Multi / Tie | 26 / 4 / 19 (out of 49 retries) |
| **Decomposer** | Complex EX | 26.7% (30 complex questions) |

### Key Findings

1. **RAG is the biggest lever** — +11pp over baseline (23.4% → 34.6%)
2. **Strong model matters** — Claude Opus 4.7 EX 47.0% vs DeepSeek 39.0% (+8pp)
3. **Decomposer ineffective** for DeepSeek — complex question EX lower than simple
4. **Self-Correction** fix rate improved from 7-20% to **24-27%**
5. **Guard** is form-only — 55% of passed SQL still produce wrong results

## Tech Stack

| Layer | Technology |
|-------|-----------|
| LLM | DeepSeek V4 Pro / OpenAI GPT-4o / Claude Opus 4.7 |
| Embedding | BAAI/bge-small-zh-v1.5 (local) |
| Vector DB | ChromaDB |
| Orchestration | LangGraph + LangChain |
| AST Validation | sqlglot (Python) + vitess/sqlparser (Go) |
| Databases | SQLite / PostgreSQL 16 / MySQL 8.4 |
| Cache + State | Redis 7 |
| Message Queue | Kafka 3.7 (KRaft, no ZooKeeper) |
| API | FastAPI + Pydantic v2 |
| MCP Protocol | fastmcp (Python) + mark3labs/mcp-go (Go) |
| Gateway | go-chi/chi (Go) |
| Frontend | Streamlit 1.51 |
| Observability | OpenTelemetry + TraceLogger (jsonl) |
| Deployment | Docker Compose (6 services) |

## Project Structure

```
nl2sql-agent/
  agent/                   # LangGraph nodes + graphs + state
    nodes/                 # router, generator, guard, voter, refiner, etc.
    graphs/                # full_graph, feedback_graph
  api/                     # FastAPI app + routes (task, query, eval, health)
  worker/                  # Kafka consumer (independent process)
  infrastructure/          # broker (Kafka) + task_store (Redis state machine)
  guard/                   # safety_rules (9 rules) + error_types + error_classifier
  tools/
    mcp/                   # Python MCP tools (validate_sql, execute_readonly_sql)
    mcp-server-go/         # Go MCP Server (vitess/sqlparser + database/sql)
    sql_executor.py        # SQL execution engine
  gateway/                 # Go API Gateway (go-chi rate-limit + reverse proxy)
  retrieval/               # RAG pipeline (schema + domain + sample rows)
  observability/           # TraceLogger + OTel bridge
  evaluation/              # BIRD evaluation runner
  storage/                 # Redis cache + config + db_registry
  configs/                 # Prompt versions + system prompts
  corpus/bird_fewshot/     # Few-shot examples (by db_id + dialect)
  deployment/              # Dockerfile + docker-compose.yml
  tests/                   # 14 test files (188+ tests)
  ui/                      # Streamlit app (ChatGPT-style)
```

## Evaluation Commands

```bash
# Quick test (20 samples, 2 configs)
python -m evaluation.run --test --samples 20 --configs R2,R5

# Full ablation (500 samples, 8 workers, checkpoint resume)
python -m evaluation.run --exp ablation --max-workers 8

# Pre-compute gold SQL cache
python -m evaluation.precompute_gold

# Multi-DB smoke test
python tests/smoke_multidb.py

# Run all tests
python -m pytest tests/ -v
```

## LLM Configuration

4 presets available in Streamlit sidebar. Keys stored in `llm_keys.json` (git-ignored):

```json
{"deepseek": "sk-xxx", "openai": "sk-xxx", "anthropic": "sk-ant-xxx"}
```

| Provider | Model | Notes |
|----------|-------|-------|
| DeepSeek V4 Pro | deepseek-v4-pro | Default, OpenAI-compatible |
| OpenAI GPT-4o | gpt-4o | Standard OpenAI API |
| Claude Opus 4.7 | claude-opus-4-7 | Auto-switches to ChatAnthropic |
| Custom | (any) | OpenAI-compatible base_url |

## Bring Your Own Database

Edit `databases.json`:

```json
{
  "databases": [
    {"db_id": "my_mysql", "display_name": "Production MySQL",
     "database_url": "mysql+pymysql://user:pass@host:3306/db"},
    {"db_id": "my_pg", "display_name": "Analytics PG",
     "database_url": "postgresql+psycopg2://user:pass@host:5432/db"}
  ]
}
```

Dialect, table count, and online status auto-detected. Save and refresh — databases appear in the dropdown.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LLM_BASE_URL` | `https://api.deepseek.com/v1` | LLM API URL |
| `LLM_CHAT_MODEL` | `deepseek-v4-pro` | Default model |
| `LLM_API_KEY` | (from `llm_keys.json` or env) | API key |
| `DATABASE_URL` | `sqlite:///./data/demo.db` | Default database |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection |
| `EMBED_MODEL_NAME` | `BAAI/bge-small-zh-v1.5` | Embedding model |
| `CHROMA_PATH` | `./data/chroma_db` | ChromaDB path |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
