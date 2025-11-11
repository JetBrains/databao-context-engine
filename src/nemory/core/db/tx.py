from contextlib import contextmanager

import duckdb


@contextmanager
def transaction(conn: duckdb.DuckDBPyConnection):
    conn.execute("BEGIN")
    try:
        yield
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
