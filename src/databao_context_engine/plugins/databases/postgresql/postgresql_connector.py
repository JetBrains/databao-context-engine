from __future__ import annotations

import logging
from contextlib import AbstractContextManager
from typing import Any

from databao_context_engine.plugins.databases.base_connector import BaseConnector
from databao_context_engine.plugins.databases.postgresql.config_file import (
    PostgresConfigFile,
    PostgresConnectionProperties,
)
from databao_context_engine.plugins.databases.postgresql.sync_asyncpg_connection import SyncAsyncpgConnection

logger = logging.getLogger(__name__)


class PostgresqlConnector(BaseConnector[PostgresConfigFile]):
    def connect(self, file_config: PostgresConfigFile, *, catalog: str | None = None) -> AbstractContextManager[Any]:
        kwargs = self._create_connection_kwargs(file_config.connection)
        if catalog:
            kwargs["database"] = catalog
        return SyncAsyncpgConnection(kwargs)

    def execute(self, connection: SyncAsyncpgConnection, sql: str, params) -> list[dict]:
        return connection.fetch_rows(sql, params)

    def _create_connection_kwargs(self, connection_config: PostgresConnectionProperties) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "host": connection_config.host,
            "port": connection_config.port or 5432,
            "database": connection_config.database or "postgres",
        }

        if connection_config.user:
            kwargs["user"] = connection_config.user
        if connection_config.password:
            kwargs["password"] = connection_config.password
        kwargs.update(connection_config.additional_properties or {})
        return kwargs
