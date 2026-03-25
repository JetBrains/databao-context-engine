from __future__ import annotations

from contextlib import AbstractContextManager, closing
from typing import Any

import pymysql
from pymysql.constants import CLIENT

from databao_context_engine.plugins.databases.base_connector import BaseConnector
from databao_context_engine.plugins.databases.mysql.config_file import MySQLConfigFile


class MySQLConnector(BaseConnector[MySQLConfigFile]):
    def connect(self, file_config: MySQLConfigFile, *, catalog: str | None = None) -> AbstractContextManager[Any]:
        connection_kwargs = file_config.connection.to_pymysql_kwargs()

        if catalog:
            connection_kwargs["database"] = catalog

        return closing(
            pymysql.connect(
                **connection_kwargs,
                cursorclass=pymysql.cursors.DictCursor,
                client_flag=CLIENT.MULTI_STATEMENTS | CLIENT.MULTI_RESULTS,
            )
        )

    def execute(self, connection, sql: str, params) -> list[dict]:
        with connection.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [{k.lower(): v for k, v in row.items()} for row in rows]
