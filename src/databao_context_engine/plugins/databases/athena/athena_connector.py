from __future__ import annotations

from contextlib import AbstractContextManager, closing
from typing import Any

from pyathena import connect
from pyathena.cursor import DictCursor

from databao_context_engine.plugins.databases.athena.config_file import AthenaConfigFile
from databao_context_engine.plugins.databases.base_connector import BaseConnector


class AthenaConnector(BaseConnector[AthenaConfigFile]):
    def connect(self, file_config: AthenaConfigFile, *, catalog: str | None = None) -> AbstractContextManager[Any]:
        return closing(connect(**file_config.connection.to_athena_kwargs(), cursor_class=DictCursor))

    def execute(self, connection, sql: str, params) -> list[dict]:
        with connection.cursor() as cur:
            cur.execute(sql, params or {})
            return cur.fetchall()
