from __future__ import annotations

from contextlib import AbstractContextManager, closing
from pathlib import Path
from typing import Any

import duckdb

from databao_context_engine.plugins.databases.base_connector import BaseConnector
from databao_context_engine.plugins.databases.duckdb.config_file import DuckDBConfigFile
from databao_context_engine.plugins.duckdb_tools import fetchall_dicts


class DuckDBConnector(BaseConnector[DuckDBConfigFile]):
    def connect(self, file_config: DuckDBConfigFile, *, catalog: str | None = None) -> AbstractContextManager[Any]:
        duckdb_path = Path(file_config.connection.database_path)
        if not duckdb_path.is_file():
            raise ConnectionError(f"No DuckDB database was found at path {duckdb_path.resolve()}")

        database_path = str(duckdb_path.resolve())
        return closing(duckdb.connect(database=database_path, read_only=True))

    def execute(self, connection, sql: str, params) -> list[dict]:
        cur = connection.cursor()
        return fetchall_dicts(cur, sql, params)
