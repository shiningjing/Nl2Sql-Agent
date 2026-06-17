"""Multi-database smoke test — 3 questions each on SQLite / MySQL / PG."""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent.graphs.full_graph import create_full_graph
from agent.state import AgentState
from agent.llm_factory import set_llm_config, clear_llm_config

CASES = [
    # SQLite (BIRD)
    {"db": "california_schools", "url": "sqlite:///F:/Experience/nl2sql-mini-agent/data/bird/mini_dev_data/minidev/MINIDEV/dev_databases/california_schools/california_schools.sqlite",
     "questions": [
         "How many schools are there?",
         "What is the average FRPM count by county?",
         "List the top 5 schools by number of test takers.",
     ]},
    # MySQL Demo
    {"db": "mysql_demo", "url": "mysql+pymysql://nl2sql:nl2sql@127.0.0.1:3306/demo",
     "questions": [
         "How many customers are there?",
         "What is the total sales amount per order status?",
         "List the top 5 customers by total order amount.",
     ]},
    # PostgreSQL Demo
    {"db": "pg_demo", "url": "postgresql+psycopg2://nl2sql:nl2sql@127.0.0.1:5432/demo",
     "questions": [
         "How many products are there?",
         "What is the average rating per product category?",
         "Find products that have never been reviewed.",
     ]},
]

graph = create_full_graph()
results = []

for case in CASES:
    for i, q in enumerate(case["questions"]):
        t0 = time.time()
        state: AgentState = {
            "question": q,
            "db_id": case["db"],
            "database_url": case["url"],
            "rag_schema": True,
            "rag_domain": True,
            "rag_hybrid": True,
            "rag_k": 8,
            "rag_fk_expand": True,
            "rag_column_prune": False,
            "multi_candidate": False,
            "fewshot_enabled": True,
            "complexity": "simple",
            "loop_count": 0,
            "max_loops": 3,
        }
        try:
            set_llm_config(model="deepseek-v4-pro", api_key="", base_url="")
            result = graph.invoke(state)
            sql = result.get("sql", "")
            exec_result = result.get("exec_result") or {}
            success = exec_result.get("success", False)
            row_count = exec_result.get("row_count", 0)
            error = exec_result.get("error", "")
            elapsed = time.time() - t0
            status = "OK" if success else "FAIL"
            info = f"rows={row_count}" if success else f"err={error[:50]}"
            print(f"  [{status}] [{case['db']:25s}] Q{i+1}: {q[:50]:50s} {info}  ({elapsed:.1f}s)")
        except Exception as e:
            elapsed = time.time() - t0
            print(f"  [ERR] [{case['db']:25s}] Q{i+1}: {q[:50]:50s} {e!s:50s}  ({elapsed:.1f}s)")
        finally:
            clear_llm_config()

print("\nDone.")
