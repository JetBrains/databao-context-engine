from typing import Any

from duckdb import DuckDBPyConnection

from databao_context_engine.pluginlib.config import DuckDBSecret


def generate_create_secret_sql(secret_name, duckdb_secret: DuckDBSecret) -> str:
    parameters = [("type", duckdb_secret.type)] + list(duckdb_secret.properties.items())
    return f"""CREATE SECRET {secret_name} (
    {", ".join([f"{k} '{v}'" for (k, v) in parameters])}
);
"""


def fetchone_dicts(cur: DuckDBPyConnection, sql: str, params: list | None = None) -> dict[str, Any] | None:
    cur.execute(sql, params or [])
    columns = [desc[0].lower() for desc in cur.description] if cur.description else []
    row = cur.fetchone()
    return dict(zip(columns, row)) if row else None


def fetchall_dicts(cur: DuckDBPyConnection, sql: str, params: list | None = None) -> list[dict[str, Any]]:
    cur.execute(sql, params or [])
    columns = [desc[0].lower() for desc in cur.description] if cur.description else []
    rows = cur.fetchall()
    return [dict(zip(columns, row)) for row in rows]
