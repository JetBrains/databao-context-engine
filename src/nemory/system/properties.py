import os
from pathlib import Path

# it's private, so it doesn't get imported directy. This value is mocked in tests
_nemory_path = Path(os.getenv("NEMORY_PATH") or "~/.nemory").expanduser().resolve()


def get_nemory_path() -> Path:
    return _nemory_path


def get_db_path(nemory_path: Path = get_nemory_path()) -> Path:
    return nemory_path / "nemory.duckdb"
