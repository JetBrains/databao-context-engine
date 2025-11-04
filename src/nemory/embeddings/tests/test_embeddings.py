from pathlib import Path

from nemory.embeddings.impl.storage import (
    load_embeddings_schema_sql,
    load_vss_extension_sql,
    init_storage,
    connect
)


def test_embeddings_schema_loaded():
    assert load_embeddings_schema_sql()


def test_vss_extension_loaded():
    assert load_vss_extension_sql()


def test_init_duckdb(tmp_path: Path):
    db_file = tmp_path / "test_db.duckdb"
    init_storage(db_file)
    with connect(db_file) as connection:
        tables = connection.sql(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_catalog = ?
            """,
            params=["test_db"]
        ).fetchall()
        assert 'embeddings' in [table[0] for table in tables]
