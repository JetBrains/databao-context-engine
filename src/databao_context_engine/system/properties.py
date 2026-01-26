import os
from pathlib import Path

from databao_context_engine.project.layout import get_output_dir

# it's private, so it doesn't get imported directy. This value is mocked in tests
_dce_path = Path(os.getenv("DATABAO_CONTEXT_ENGINE_PATH") or "~/.dce").expanduser().resolve()


def get_dce_path() -> Path:
    return _dce_path


def get_db_path(project_dir: Path) -> Path:
    return get_output_dir(project_dir) / "dce.duckdb"
