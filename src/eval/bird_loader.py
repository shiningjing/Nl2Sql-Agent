"""BIRD Mini-Dev dataset loader.

Loads 500 text-to-SQL pairs across 11 SQLite databases from the BIRD Mini-Dev
dataset. Each sample carries its own database path — no shared demo.db.

Download: extract minidev.zip → data/bird/mini_dev_data/
"""
import json
import os
from dataclasses import dataclass
from pathlib import Path


def _default_data_dir() -> str:
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "data", "bird", "mini_dev_data", "minidev", "MINIDEV",
    )


@dataclass
class BirdSample:
    question_id: str
    question: str
    gold_sql: str
    db_id: str
    difficulty: str          # simple / moderate / challenging
    evidence: str            # human-written hints from BIRD experts
    database_path: str       # absolute path to .sqlite file


def load_bird_dev(data_dir: str | None = None) -> list[BirdSample]:
    """Parse BIRD mini_dev_sqlite.json and resolve database paths.

    Args:
        data_dir: path to the MINIDEV directory containing dev_databases/ and
                  mini_dev_sqlite.json. Defaults to data/bird/mini_dev_data/minidev/MINIDEV/

    Returns list of BirdSample, each with a resolved .sqlite path.
    """
    root = data_dir or _default_data_dir()
    json_path = os.path.join(root, "mini_dev_sqlite.json")
    db_dir = os.path.join(root, "dev_databases")

    with open(json_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    samples = []
    for item in raw:
        db_id = item["db_id"]
        db_path = os.path.join(db_dir, db_id, f"{db_id}.sqlite")
        if not os.path.exists(db_path):
            # Some databases use alternative naming
            alt_dir = os.path.join(db_dir, db_id)
            candidates = list(Path(alt_dir).glob("*.sqlite"))
            if candidates:
                db_path = str(candidates[0])
            else:
                raise FileNotFoundError(
                    f"Database not found for db_id={db_id}: expected {db_path}"
                )

        samples.append(BirdSample(
            question_id=str(item["question_id"]),
            question=item["question"],
            gold_sql=item["SQL"],
            db_id=db_id,
            difficulty=item.get("difficulty", "?"),
            evidence=item.get("evidence", ""),
            database_path=db_path,
        ))

    return samples


def get_database_url(sample: BirdSample) -> str:
    """Convert sample to SQLAlchemy connection URL."""
    # Use absolute path with sqlite:/// (3 slashes for absolute)
    # Windows backslashes cause SQLAlchemy hangs in multi-threaded envs
    return f"sqlite:///{sample.database_path.replace(chr(92), '/')}"


def get_stats(samples: list[BirdSample]) -> dict:
    """Return summary statistics for a list of samples."""
    db_ids = set()
    diff_counts = {"simple": 0, "moderate": 0, "challenging": 0}
    for s in samples:
        db_ids.add(s.db_id)
        d = s.difficulty.lower() if s.difficulty else "?"
        if d in diff_counts:
            diff_counts[d] += 1
    return {
        "total": len(samples),
        "databases": len(db_ids),
        "db_ids": sorted(db_ids),
        "difficulty": diff_counts,
    }
