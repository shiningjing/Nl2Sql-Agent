# NL2SQL Agent

Natural language to SQL based on LangGraph with RAG-augmented schema retrieval and self-correcting execution loops.

## Architecture

LangGraph state machine with 9 nodes, 4 conditional edges, and a retry loop.

```
Router → Schema Retriever → Decomposer → Few-shot Selector → Generator
                                                                    │
                                                    ┌───────────────▼───────────────┐
                                                    │           Generator           │
                                                    │  multi-candidate (0/0.3/0.6)  │
                                                    └───────────────┬───────────────┘
                                                                    │
                                                    ┌───────────────▼───────────────┐
                                                    │            Guard              │
                                                    │  syntax check + hallucination │
                                                    └───────────────┬───────────────┘
                                                                    │
                                                         pass ──────┴────── fail
                                                           │                  │
                                                ┌──────────▼──────────┐  ┌──────▼──────┐
                                                │       Voter         │  │   Refiner   │
                                                │  parallel exec +    │  │  error →    │
                                                │  LLM fallback       │  │  Generator  │
                                                └──────────┬──────────┘  └─────────────┘
                                                           │
                                                   winner ─┴── no winner → Refiner
                                                           │
                                                ┌──────────▼──────────┐
                                                │   Semantic Check    │
                                                │   LLM YES/NO        │
                                                └──────────┬──────────┘
                                                           │
                                                    YES ───┴─── NO → Refiner
                                                           │
                                                          END
```

## Quick Start

```bash
pip install -r requirements.txt
cp .env.example .env   # add LLM_API_KEY
python scripts/ingest.py
streamlit run app.py
```

## BIRD Mini-Dev Results (500 samples, 11 databases)

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
|--------|-------------|----------------|-------------------|
| R0_Baseline | 37.8% | 20.8% | 8.8% |
| R2_RAG | 48.6% | 30.8% | 23.5% |
| R4_PruneFewshot | 52.0% | 36.0% | 19.6% |
| R5_Evidence | 49.3% | 38.0% | 25.5% |

## Evaluation

```bash
# Quick test
python scripts/eval_bird.py --test --samples 20 --configs R2,R5

# Full ablation (with checkpoint resume)
python scripts/eval_bird.py --exp ablation --max-workers 8

# Pre-compute gold SQL cache (one-time)
python scripts/_precompute_gold.py
```

## Tech Stack

| Layer | Choice |
|-------|--------|
| LLM | DeepSeek API (ChatOpenAI) |
| Embedding | BAAI/bge-small-zh-v1.5 (local) |
| Vector DB | ChromaDB |
| Orchestration | LangGraph + LangChain |
| Databases | SQLite / PostgreSQL / MySQL (SQLAlchemy 2.0) |
| AST Guard | sqlglot |
| API | FastAPI + Pydantic |
| Cache | Redis (semantic + schema) |
| Frontend | Streamlit |
| Deploy | Docker Compose |

## Environment

| Variable | Default |
|----------|---------|
| `LLM_BASE_URL` | `https://api.deepseek.com/v1` |
| `LLM_CHAT_MODEL` | `deepseek-v4-pro` |
| `SQL_DIALECT` | `sqlite` |
| `DATABASE_URL` | `sqlite:///./data/demo.db` |
