"""Database registry: db_id -> database_url + metadata for BIRD + user config."""
import json
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from sqlalchemy import create_engine, text


@dataclass
class DbInfo:
    db_id: str
    display_name: str
    database_url: str
    domain: str
    table_count: int
    dialect: str = ""       # sqlite / mysql / postgresql
    online: bool = True     # False if container unreachable


_FRIENDLY_NAMES = {
    "california_schools": "California Schools",
    "card_games": "Card Games",
    "codebase_community": "Codebase Community",
    "debit_card_specializing": "Debit Card Specializing",
    "european_football_2": "European Football 2",
    "financial": "Financial",
    "formula_1": "Formula 1",
    "mysql_demo": "MySQL Demo",
    "pg_demo": "PostgreSQL Demo",
    "student_club": "Student Club",
    "superhero": "Superhero",
    "thrombosis_prediction": "Thrombosis Prediction",
    "toxicology": "Toxicology",
}

_DOMAINS = {
    "california_schools": "California public schools — test scores, FRPM, demographics",
    "card_games": "Trading card game rules, sets, legalities",
    "codebase_community": "Stack Overflow-style community — posts, users, badges, votes",
    "debit_card_specializing": "Banking — customers, transactions, gas stations, products",
    "european_football_2": "European soccer — leagues, matches, players, teams",
    "financial": "Bank financial — accounts, loans, transactions, clients",
    "formula_1": "F1 racing — circuits, drivers, results, lap times",
    "student_club": "University student clubs — members, events, budget",
    "superhero": "Comic superheroes — attributes, powers, alignment",
    "thrombosis_prediction": "Medical — thrombosis prediction lab data",
    "toxicology": "Chemical toxicology — molecules, atoms, bonds",
    "mysql_demo": "MySQL Demo — customers, orders, products, reviews",
    "pg_demo": "PostgreSQL Demo — customers, orders, products, reviews",
}

_TABLE_COUNTS = {
    "california_schools": 3, "card_games": 6, "codebase_community": 8,
    "debit_card_specializing": 5, "european_football_2": 7, "financial": 8,
    "formula_1": 13, "student_club": 8, "superhero": 10,
    "thrombosis_prediction": 3, "toxicology": 4,
    "mysql_demo": 6, "pg_demo": 6,
}

# Docker demo database URLs (containers started via `docker compose up -d postgres mysql`)
_DOCKER_DBS = {
    "mysql_demo": "mysql+pymysql://nl2sql:nl2sql@127.0.0.1:3306/demo",
    "pg_demo": "postgresql+psycopg2://nl2sql:nl2sql@127.0.0.1:5432/demo",
}


def _get_dialect(database_url: str) -> str:
    for prefix, dialect in [
        ("postgresql", "postgresql"),
        ("mysql", "mysql"),
        ("sqlite", "sqlite"),
    ]:
        if database_url.startswith(prefix):
            return dialect
    return "unknown"


def _get_data_dir() -> str:
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "bird", "mini_dev_data", "minidev", "MINIDEV",
    )


def get_database_url(db_id: str) -> str:
    if db_id in _DOCKER_DBS:
        return _DOCKER_DBS[db_id]
    db_path = os.path.join(_get_data_dir(), "dev_databases", db_id, f"{db_id}.sqlite")
    db_path = os.path.abspath(db_path)
    return f"sqlite:///{db_path}"


def _check_online(database_url: str) -> bool:
    """Quick connectivity check — return False if the DB is unreachable."""
    try:
        engine = create_engine(database_url, connect_args={"connect_timeout": 3} if "mysql" in database_url else {})
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return True
    except Exception:
        return False


_USER_DBS_PATH = Path(__file__).resolve().parent.parent / "databases.json"


def _load_user_databases() -> list[DbInfo]:
    """Load user-configured databases from databases.json."""
    try:
        with open(_USER_DBS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

    result = []
    builtin_ids = set(_FRIENDLY_NAMES.keys())
    for entry in data.get("databases", []):
        db_id = entry.get("db_id", "").strip()
        if not db_id:
            continue
        if db_id in builtin_ids:
            continue  # skip if user accidentally uses a built-in id
        url = entry.get("database_url", "")
        online = _check_online(url)
        table_count = entry.get("table_count", 0)
        if table_count == 0 and online:
            table_count = _count_tables(url)
        result.append(DbInfo(
            db_id=db_id,
            display_name=entry.get("display_name", db_id),
            database_url=url,
            domain=entry.get("domain", ""),
            table_count=table_count,
            dialect=_get_dialect(url),
            online=online,
        ))
    return result


def _count_tables(database_url: str) -> int:
    """Quick table count for a live database."""
    try:
        engine = create_engine(database_url)
        with engine.connect() as conn:
            if database_url.startswith("sqlite"):
                result = conn.execute(
                    text("SELECT count(*) FROM sqlite_master WHERE type='table'")
                )
            elif database_url.startswith("postgresql"):
                result = conn.execute(
                    text("SELECT count(*) FROM information_schema.tables WHERE table_schema='public'")
                )
            elif database_url.startswith("mysql"):
                result = conn.execute(
                    text("SELECT count(*) FROM information_schema.tables WHERE table_schema=DATABASE()")
                )
            else:
                engine.dispose()
                return 0
            count = result.scalar()
        engine.dispose()
        return count
    except Exception:
        return 0


@lru_cache(maxsize=1)
def list_databases() -> list[DbInfo]:
    result = []
    # BIRD SQLite + Docker demo databases
    for db_id, name in sorted(_FRIENDLY_NAMES.items()):
        url = get_database_url(db_id)
        online = True
        if db_id in _DOCKER_DBS:
            online = _check_online(url)
        result.append(DbInfo(
            db_id=db_id,
            display_name=name,
            database_url=url,
            domain=_DOMAINS.get(db_id, ""),
            table_count=_TABLE_COUNTS.get(db_id, 0),
            dialect=_get_dialect(url),
            online=online,
        ))
    # Append user-configured databases from databases.json
    result.extend(_load_user_databases())
    return result
