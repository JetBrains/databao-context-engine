from duckdb import DuckDBPyConnection

from databao_context_engine.services.table_name_policy import TableNamePolicy


def before_migration(conn: DuckDBPyConnection) -> None:
    rows = conn.execute("SELECT table_name FROM embedding_model_registry").fetchall()
    for (table_name,) in rows:
        TableNamePolicy.validate_table_name(table_name=table_name)
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    conn.execute("DELETE FROM embedding_model_registry")
