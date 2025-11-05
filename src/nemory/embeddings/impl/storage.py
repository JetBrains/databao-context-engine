from importlib.resources import read_text
from pathlib import Path

import duckdb

def init_storage(database_path: str | Path):
    with connect(database_path, read_only=False) as connection:
        vss_extension_sql = load_vss_extension_sql()
        schemq_sql = load_embeddings_schema_sql()
        connection.execute(vss_extension_sql)
        connection.execute(schemq_sql)


def connect(database_path: str | Path, read_only=True):
    return duckdb.connect(database=database_path, read_only=read_only)


def load_embeddings_schema_sql():
    return read_text(__name__, "schema.sql")


def load_vss_extension_sql():
    return read_text(__name__, "vss.sql")
