from importlib.resources import read_text

import duckdb


def connect(database_path: str):
    return duckdb.connect(database=database_path, read_only=True)


def load_embeddings_schema_sql():
    return read_text("nemory.embeddings.impl", "schema.sql")


def load_vss_extension_sql():
    return read_text("nemory.embeddings.impl", "vss.sql")
