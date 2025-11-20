from contextlib import contextmanager
from pathlib import Path

import duckdb

from nemory.system.properties import get_db_path


@contextmanager
def open_duckdb_connection(db_path: str | Path | None = None):
    """
    Open a DuckDB connection with vector search enabled and close on exist.
    Loads the vss extension and enables HNSW experimental persistence.

    Usage:
        with open_duckdb_connection() as conn:
    """
    path = str(db_path or get_db_path())
    conn = duckdb.connect(path)
    try:
        conn.execute("LOAD vss;")
        conn.execute("SET hnsw_enable_experimental_persistence = true;")
        yield conn
    finally:
        conn.close()
