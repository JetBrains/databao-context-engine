import re
from pathlib import Path

import duckdb
import pytest

from nemory.storage.migrate import MigrationDTO, MigrationError, load_migrations, migrate


def _test_migration_path(migration_name) -> Path:
    return Path(__file__).parent / "test_migrations" / migration_name


m1_file = _test_migration_path("V01__init.sql")
m2_file = _test_migration_path("V02__second.sql")
m3_file = _test_migration_path("V03__third.sql")
m1 = MigrationDTO(name="V01__init.sql", version=1, checksum="35e5a5b1c86224ce5020561e075a8689")
m2 = MigrationDTO(name="V02__second.sql", version=2, checksum="4584270cfb055f3a8f7c9c4f9a608ca5")
m3 = MigrationDTO(name="V03__third.sql", version=3, checksum="308d96a49eefad4f1514ac746b3ac0c3")

m2_duplicate_file = _test_migration_path("V02__second_duplicate.sql")


@pytest.fixture
def db_path(request, tmp_path: Path) -> Path:
    return tmp_path / f"{request.function.__name__}.duckdb"


def assert_tables(db_path: Path, *expected_tables):
    conn = duckdb.connect(db_path)
    all_tables = [table for (table,) in conn.execute("SELECT table_name FROM information_schema.tables").fetchall()]
    tables = set(all_tables) - {"migration_history"}
    assert tables == set(expected_tables)


def load_applied_migrations(db_path: Path) -> list[MigrationDTO]:
    with duckdb.connect(db_path) as conn:
        return load_migrations(conn)


def test_migrate_on_empty(db_path: Path) -> None:
    migrate(db_path, [m1_file, m2_file])
    assert_tables(db_path, "test_1", "test_2")
    assert [m1, m2] == load_applied_migrations(db_path)


def test_migrate_on_non_empty(db_path: Path) -> None:
    migrate(db_path, [m1_file])
    assert_tables(db_path, "test_1")
    assert [m1] == load_applied_migrations(db_path)

    migrate(db_path, [m1_file, m2_file, m3_file])
    assert_tables(db_path, "test_1", "test_2", "test_3")
    assert [m1, m2, m3] == load_applied_migrations(db_path)


def test_migrate_twice_does_nothing(db_path: Path) -> None:
    migrate(db_path, [m1_file])
    assert_tables(db_path, "test_1")
    assert [m1] == load_applied_migrations(db_path)

    migrate(db_path, [m1_file])


def test_migrate_duplicated_name(db_path: Path) -> None:
    migrate(db_path, [m1_file, m2_file])
    with pytest.raises(MigrationError, match=re.escape("Migrations with versions [2] already exist")):
        migrate(db_path, [m2_duplicate_file])
    assert_tables(db_path, "test_1", "test_2")
    assert [m1, m2] == load_applied_migrations(db_path)
