from __future__ import annotations

from contextlib import AbstractContextManager, closing
from typing import Any, Mapping

from mssql_python import connect  # type: ignore[import-untyped]

from databao_context_engine.plugins.databases.base_connector import BaseConnector
from databao_context_engine.plugins.databases.mssql.config_file import MSSQLConfigFile


class MSSQLConnector(BaseConnector[MSSQLConfigFile]):
    def connect(self, file_config: MSSQLConfigFile, *, catalog: str | None = None) -> AbstractContextManager[Any]:
        connection = file_config.connection

        connection_kwargs = connection.to_mssql_kwargs()
        if catalog:
            connection_kwargs["database"] = catalog

        connection_string = self._create_connection_string_for_config(connection_kwargs)
        return closing(connect(connection_string))

    def execute(self, connection, sql: str, params) -> list[dict]:
        with connection.cursor() as cursor:
            cursor.execute(sql, params or ())
            if not cursor.description:
                return []

            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def _create_connection_string_for_config(self, file_config: Mapping[str, Any]) -> str:
        def _escape_odbc_value(value: str) -> str:
            return "{" + value.replace("}", "}}").replace("{", "{{") + "}"

        host = file_config.get("host")
        if not host:
            raise ValueError("A host must be provided to connect to the MSSQL database.")

        port = file_config.get("port", 1433)
        instance = file_config.get("instanceName")
        if instance:
            server_part = f"{host}\\{instance}"
        else:
            server_part = f"{host},{port}"

        database = file_config.get("database")
        user = file_config.get("user")
        password = file_config.get("password")

        connection_parts = {
            "server": _escape_odbc_value(server_part),
            "database": _escape_odbc_value(str(database)) if database is not None else None,
            "uid": _escape_odbc_value(str(user)) if user is not None else None,
            "pwd": _escape_odbc_value(str(password)) if password is not None else None,
            "encrypt": file_config.get("encrypt"),
            "trust_server_certificate": "yes" if file_config.get("trust_server_certificate") else None,
        }

        return ";".join(f"{k}={v}" for k, v in connection_parts.items() if v is not None)
