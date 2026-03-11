import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import duckdb
from duckdb import DuckDBPyConnection

logger = logging.getLogger(__name__)


@contextmanager
def open_duckdb_connection(db_path: str | Path) -> Iterator[DuckDBPyConnection]:
    """Open a DuckDB connection with search extensions enabled and close on exist.

    It installs and loads the `vss` and `fts` extensions, and enables HNSW
    experimental persistence on DuckDB.

    Usage:
        with open_duckdb_connection() as conn:

    Yields:
        The opened DuckDB connection.

    """
    path = str(db_path)
    conn = duckdb.connect(path)
    logger.debug(f"Connected to DuckDB database at {path}")

    try:
        conn.execute("INSTALL fts;")
        conn.execute("LOAD fts;")
        conn.execute("INSTALL vss;")
        conn.execute("LOAD vss;")
        conn.execute("SET hnsw_enable_experimental_persistence = true;")

        logger.debug("Loaded full-text and vector search extensions")
        yield conn
    finally:
        conn.close()
