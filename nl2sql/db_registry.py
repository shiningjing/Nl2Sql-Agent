"""Database registry: db_id -> database_url + metadata for BIRD Mini-Dev."""
import os
from dataclasses import dataclass
from functools import lru_cache


@dataclass
class DbInfo:
    db_id: str
    display_name: str
    database_url: str
    domain: str
    table_count: int

_FRIENDLY_NAMES = {
    "california_schools": "California Schools",
    "card_games": "Card Games",
    "codebase_community": "Codebase Community",
    "debit_card_specializing": "Debit Card Specializing",
    "european_football_2": "European Football 2",
    "financial": "Financial",
    "formula_1": "Formula 1",
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
}

_TABLE_COUNTS = {
    "california_schools": 3, "card_games": 6, "codebase_community": 8,
    "debit_card_specializing": 5, "european_football_2": 7, "financial": 8,
    "formula_1": 13, "student_club": 8, "superhero": 10,
    "thrombosis_prediction": 3, "toxicology": 4,
}


def _get_data_dir() -> str:
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "bird", "mini_dev_data", "minidev", "MINIDEV",
    )


def get_database_url(db_id: str) -> str:
    db_path = os.path.join(_get_data_dir(), "dev_databases", db_id, f"{db_id}.sqlite")
    db_path = os.path.abspath(db_path)
    return f"sqlite:///{db_path}"


@lru_cache(maxsize=1)
def list_databases() -> list[DbInfo]:
    result = []
    for db_id, name in sorted(_FRIENDLY_NAMES.items()):
        result.append(DbInfo(
            db_id=db_id,
            display_name=name,
            database_url=get_database_url(db_id),
            domain=_DOMAINS.get(db_id, ""),
            table_count=_TABLE_COUNTS.get(db_id, 0),
        ))
    return result
