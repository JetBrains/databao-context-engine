from __future__ import annotations

from contextlib import AbstractContextManager, closing
from datetime import datetime
from typing import Any

import snowflake.connector

from databao_context_engine.plugins.databases.base_connector import BaseConnector
from databao_context_engine.plugins.databases.snowflake.config_file import SnowflakeConfigFile


class SnowflakeConnector(BaseConnector[SnowflakeConfigFile]):
    def connect(self, file_config: SnowflakeConfigFile, *, catalog: str | None = None) -> AbstractContextManager[Any]:
        connection = file_config.connection
        snowflake.connector.paramstyle = "qmark"
        connection_kwargs = connection.to_snowflake_kwargs()
        if catalog:
            connection_kwargs["database"] = catalog

        return closing(
            snowflake.connector.connect(
                **connection_kwargs,
            )
        )

    def execute(self, connection, sql: str, params) -> list[dict]:
        def normalize_value(v):
            if isinstance(v, datetime):
                return v.isoformat()
            return v

        with connection.cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [{k.lower(): normalize_value(v) for k, v in row.items()} for row in rows]
