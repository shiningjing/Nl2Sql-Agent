"""Quick peek at demo table structures."""
from sqlalchemy import create_engine, text

print("=== MySQL Demo ===")
my = create_engine("mysql+pymysql://nl2sql:nl2sql@127.0.0.1:3306/demo")
with my.connect() as conn:
    tables = conn.execute(text("SHOW TABLES")).fetchall()
    for (tname,) in tables:
        cnt = conn.execute(text(f"SELECT COUNT(*) FROM `{tname}`")).scalar()
        cols = conn.execute(text(f"SHOW COLUMNS FROM `{tname}`")).fetchall()
        print(f"  {tname} ({cnt} rows): {', '.join(c[0] for c in cols)}")

print()
print("=== PostgreSQL Demo ===")
pg = create_engine("postgresql+psycopg2://nl2sql:nl2sql@127.0.0.1:5432/demo")
with pg.connect() as conn:
    tables = conn.execute(
        text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    ).fetchall()
    for (tname,) in tables:
        cnt = conn.execute(text(f'SELECT COUNT(*) FROM "{tname}"')).scalar()
        cols = conn.execute(
            text(f"SELECT column_name FROM information_schema.columns WHERE table_name='{tname}'")
        ).fetchall()
        print(f"  {tname} ({cnt} rows): {', '.join(c[0] for c in cols)}")
