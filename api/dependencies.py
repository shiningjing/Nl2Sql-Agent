"""FastAPI dependencies — shared singletons injected via Depends()."""
from storage.config import Config


def get_config() -> Config:
    return Config
