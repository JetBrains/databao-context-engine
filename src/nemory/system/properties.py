import os
from pathlib import Path

# it's private, so it doesn't get imported directy. This value is mocked in tests
_nemory_path = Path(os.getenv("NEMORY_PATH") or "~/.nemory").expanduser().resolve()
_db_file_name = "nemory.duckdb"


def get_nemory_path() -> Path:
    return _nemory_path


def get_db_path() -> Path:
    return get_nemory_path() / _db_file_name
