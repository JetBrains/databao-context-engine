from __future__ import annotations

from contextlib import AbstractContextManager, closing
from typing import Any

import clickhouse_connect

from databao_context_engine.plugins.databases.base_connector import BaseConnector
from databao_context_engine.plugins.databases.clickhouse.config_file import ClickhouseConfigFile


class ClickhouseConnector(BaseConnector[ClickhouseConfigFile]):
    def connect(self, file_config: ClickhouseConfigFile, *, catalog: str | None = None) -> AbstractContextManager[Any]:
        return closing(
            clickhouse_connect.get_client(
                **file_config.connection.to_clickhouse_kwargs(),
            )
        )

    def execute(self, connection, sql: str, params) -> list[dict]:
        res = connection.query(sql, parameters=params) if params else connection.query(sql)
        cols = [c.lower() for c in res.column_names]
        return [dict(zip(cols, row)) for row in res.result_rows]
