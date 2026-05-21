"""Test #701 with raw sqlite3 — full exec_match simulation, no SQLAlchemy."""
import sqlite3, time, json, os

DB_PATH = r"F:\Experience\nl2sql-mini-agent\data\bird\mini_dev_data\minidev\MINIDEV\dev_databases\codebase_community\codebase_community.sqlite"

gen_sql = """WITH influential_user AS(
  SELECT Id FROM users ORDER BY Reputation DESC LIMIT 1
),user_posts AS(
  SELECT Score FROM posts WHERE OwnerUserId=(SELECT Id FROM influential_user)
)SELECT ROUND(100.0*SUM(CASE WHEN Score>50 THEN 1 ELSE 0 END)/COUNT(*),2)AS pct FROM user_posts LIMIT 200"""

gold_sql = """SELECT CAST(SUM(CASE WHEN T2.Score > 50 THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(T1.Id) FROM users T1 INNER JOIN posts T2 ON T1.Id = T2.OwnerUserId INNER JOIN ( SELECT MAX(Reputation) AS max_reputation FROM users ) T3 ON T1.Reputation = T3.max_reputation"""

def run_sql(conn, sql):
    t0 = time.time()
    cur = conn.execute(sql)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description] if cur.description else []
    dt = (time.time() - t0) * 1000
    return {"success": True, "data": rows, "columns": cols, "time_ms": dt, "row_count": len(rows)}

def normalize_rows(rows, columns):
    if not rows:
        return []
    normalized = []
    for row in rows:
        norm = []
        for v in row:
            if isinstance(v, (int, float)):
                norm.append(round(float(v), 6))
            elif v is None:
                norm.append("\x00NULL\x00")
            else:
                norm.append(str(v))
        normalized.append(tuple(norm))
    return sorted(normalized, key=lambda t: tuple(str(x) for x in t))

print(f"DB: {DB_PATH}")
print(f"DB exists: {os.path.exists(DB_PATH)}")
print(f"DB size: {os.path.getsize(DB_PATH)/1024/1024:.1f} MB")
print()

print("Connecting...")
t0 = time.time()
conn = sqlite3.connect(DB_PATH)
print(f"  connected in {time.time()-t0:.3f}s")

# Quick stats
for table in ["users", "posts"]:
    cur = conn.execute(f"SELECT COUNT(*) FROM {table}")
    print(f"  {table}: {cur.fetchone()[0]} rows")

print()
print("=== gen_sql ===")
print(gen_sql[:200])
t0 = time.time()
r_gen = run_sql(conn, gen_sql)
print(f"  {r_gen['time_ms']:.1f}ms | {r_gen['row_count']} rows | data={r_gen['data']}")

print()
print("=== gold_sql ===")
print(gold_sql[:200])
t0 = time.time()
r_gold = run_sql(conn, gold_sql)
print(f"  {r_gold['time_ms']:.1f}ms | {r_gold['row_count']} rows | data={r_gold['data']}")

print()
print("=== Normalized comparison ===")
norm_gen = normalize_rows(r_gen['data'], r_gen['columns'])
norm_gold = normalize_rows(r_gold['data'], r_gold['columns'])
print(f"  gen normalized:  {norm_gen}")
print(f"  gold normalized: {norm_gold}")
print(f"  MATCH: {norm_gen == norm_gold}")

conn.close()
print("Done.")
