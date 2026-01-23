import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import duckdb
from _duckdb import DuckDBPyConnection

logger = logging.getLogger(__name__)


@contextmanager
def open_duckdb_connection(db_path: str | Path) -> Iterator[DuckDBPyConnection]:
    """Open a DuckDB connection with vector search enabled and close on exist.

    It also loads the vss extension and enables HNSW experimental persistence on the DuckDB.

    Usage:
        with open_duckdb_connection() as conn:

    Yields:
        The opened DuckDB connection.

    """
    path = str(db_path)
    conn = duckdb.connect(path)
    logger.debug(f"Connected to DuckDB database at {path}")

    try:
        conn.execute("LOAD vss;")
        conn.execute("SET hnsw_enable_experimental_persistence = true;")

        logger.debug("Loaded Vector Similarity Search extension")
        yield conn
    finally:
        conn.close()
