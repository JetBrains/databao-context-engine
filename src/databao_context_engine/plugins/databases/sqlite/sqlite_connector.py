from __future__ import annotations

import sqlite3
from contextlib import AbstractContextManager, closing
from pathlib import Path
from typing import Any

from databao_context_engine.plugins.databases.base_connector import BaseConnector
from databao_context_engine.plugins.databases.sqlite.config_file import SQLiteConfigFile


class SQLiteConnector(BaseConnector[SQLiteConfigFile]):
    def connect(self, file_config: SQLiteConfigFile, *, catalog: str | None = None) -> AbstractContextManager[Any]:
        database_path = Path(file_config.connection.database_path)
        if not database_path.is_file():
            raise ConnectionError(f"No SQLite database was found at path {database_path.resolve()}")

        conn = sqlite3.connect(database_path)
        conn.text_factory = str
        return closing(conn)

    def _connection_check_sql_query(self) -> str:
        return "SELECT name FROM sqlite_master LIMIT 1"

    def execute(self, connection, sql: str, params) -> list[dict]:
        cur = connection.cursor()
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)

        if cur.description is None:
            return []

        rows = cur.fetchall()
        out: list[dict] = []
        for r in rows:
            if isinstance(r, sqlite3.Row):
                out.append({k.lower(): r[k] for k in r.keys()})
            else:
                cols = [d[0].lower() for d in cur.description]
                out.append(dict(zip(cols, r)))
        return out
