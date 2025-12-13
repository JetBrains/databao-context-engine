import hashlib
import logging
import re
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import LiteralString

import duckdb

from nemory.system.properties import get_db_path

logger = logging.getLogger(__name__)


def migrate(db_path: str | Path | None = None, migration_files: list[Path] | None = None) -> None:
    if migration_files is None:
        migration_files = [
            migration
            for migration in files("nemory.storage.migrations").iterdir()
            if isinstance(migration, Path) and ".sql" == migration.suffix
        ]

    db = Path(db_path or get_db_path()).expanduser().resolve()
    db.parent.mkdir(parents=True, exist_ok=True)
    logger.debug("Running migrations on database: %s", db)

    migration_manager = _MigrationManager(db, migration_files)
    migration_manager.migrate()
    logger.debug("Migration complete")


@dataclass(frozen=True)
class MigrationDTO:
    name: str
    version: int
    checksum: str


class MigrationError(Exception):
    """Base class for migration errors."""


def load_migrations(conn) -> list[MigrationDTO]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT name, version, checksum, applied_at FROM migration_history",
        )
        rows = cur.fetchall()
        return [
            MigrationDTO(name=name, version=version, checksum=checksum)
            for (name, version, checksum, applied_at) in rows
        ]


def _extract_version_from_name(name: str) -> int:
    version_groups = re.findall(r"(\d+)__", name)
    if not version_groups:
        raise ValueError(f"Invalid migration name: {name}")
    return int(version_groups[0])


@dataclass(frozen=True)
class _Migration:
    name: str
    version: int
    checksum: str
    query: str


def _create_migration(file: Path) -> _Migration:
    query_bytes = file.read_bytes()
    query = query_bytes.decode("utf-8")
    checksum = hashlib.md5(query_bytes).hexdigest()
    version = _extract_version_from_name(file.name)
    return _Migration(name=file.name, version=version, checksum=checksum, query=query)


class _MigrationManager:
    _init_migration_table_sql: LiteralString = """
        CREATE SEQUENCE IF NOT EXISTS migration_history_id_seq START 1;
        
        CREATE TABLE IF NOT EXISTS migration_history (
            id              BIGINT PRIMARY KEY DEFAULT nextval('migration_history_id_seq'),
            name            TEXT NOT NULL,
            version         INTEGER NOT NULL,
            checksum        TEXT NOT NULL,
            applied_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (version)
        );
    """

    _insert_migration_sql: LiteralString = "INSERT INTO migration_history (name, version, checksum) VALUES (?, ?, ?)"

    def __init__(self, db_path: Path, migration_files: list[Path]):
        self._migration_files = migration_files
        self._db_path = db_path
        self._requested_migrations = [_create_migration(file) for file in migration_files]

    def migrate(self) -> None:
        applied_migrations: list[MigrationDTO] = self.init_db_and_load_applied_migrations()
        applied_checksums = [m.checksum for m in applied_migrations]
        applied_versions = [m.version for m in applied_migrations]
        migrations_to_apply = [m for m in self._requested_migrations if m.checksum not in applied_checksums]
        duplicated_versions = [
            migration.version for migration in migrations_to_apply if migration.version in applied_versions
        ]
        if any(duplicated_versions):
            raise MigrationError(f"Migrations with versions {duplicated_versions} already exist")
        with duckdb.connect(self._db_path) as conn:
            for migration in migrations_to_apply:
                logger.debug("Applying migration %s", migration.name)
                with conn.cursor() as cur:
                    cur.execute("START TRANSACTION;")
                    try:
                        cur.execute(migration.query)
                        cur.execute(self._insert_migration_sql, [migration.name, migration.version, migration.checksum])
                        cur.commit()
                    except Exception:
                        cur.rollback()
                        raise MigrationError(f"Failed to apply migration {migration.name}. Aborting migration process.")

    def init_db_and_load_applied_migrations(self) -> list[MigrationDTO]:
        with duckdb.connect(str(self._db_path)) as conn:
            conn.execute(self._init_migration_table_sql)
            conn.commit()
            return load_migrations(conn)
