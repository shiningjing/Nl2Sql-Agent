"""FastAPI dependencies — shared singletons injected via Depends()."""
from nl2sql.config import Config


def get_config() -> Config:
    return Config
