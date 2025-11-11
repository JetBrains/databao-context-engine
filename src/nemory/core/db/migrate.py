import logging
import os
import subprocess
from importlib.resources import files, as_file
from pathlib import Path

logger = logging.getLogger(__name__)


def migrate(db_path: str | Path | None = None) -> None:
    db = Path(db_path or os.environ.get("NEMORY_DB_PATH", "~/.nemory/nemory.duckdb")).expanduser().resolve()
    db.parent.mkdir(parents=True, exist_ok=True)

    with as_file(files("nemory.core.db.migrations")) as migdir:
        env = os.environ.copy()
        env.update(
            {
                "PYWAY_TYPE": "duckdb",
                "PYWAY_DATABASE_NAME": str(db),
                "PYWAY_DATABASE_MIGRATION_DIR": str(migdir),
                "PYWAY_TABLE": "pyway_schema_history",
                "PYWAY_DATABASE_HOST": "local",
                "PYWAY_DATABASE_USERNAME": "local",
            }
        )

        logger.debug("Running migrations")
        completed = subprocess.run(["pyway", "migrate"], env=env, text=True, capture_output=True)
        if completed.returncode != 0:
            logger.error(
                "Pyway migrate failed (exit %s)\n--- stderr ---\n%s\n--- stdout ---\n%s",
                completed.returncode,
                completed.stderr,
                completed.stdout,
            )
            raise SystemExit(f"database migrations failed (exit {completed.returncode}).")

        logger.debug("Migration complete")
        logger.debug("pyway stdout:\n%s", completed.stdout)
