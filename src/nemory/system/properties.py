from pathlib import Path


def get_db_path() -> Path:
    return Path("~/.nemory/nemory.duckdb")
