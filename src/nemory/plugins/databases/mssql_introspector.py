from __future__ import annotations

from typing import Any, Mapping

import pyodbc  # type: ignore

from nemory.plugins.databases.base_introspector import BaseIntrospector
from nemory.plugins.databases.databases_types import DatabaseColumn


class MSSQLIntrospector(BaseIntrospector):
    _IGNORED_SCHEMAS = {
        "sys",
        "information_schema",
        "db_accessadmin",
        "db_backupoperator",
        "db_datareader",
        "db_datawriter",
        "db_ddladmin",
        "db_denydatareader",
        "db_denydatawriter",
        "db_owner",
        "db_securityadmin",
    }
    _IGNORED_CATALOGS = (
        "master",
        "model",
        "msdb",
        "tempdb",
    )
    supports_catalogs = True

    def _connect(self, file_config: Mapping[str, Any]):
        connection = file_config.get("connection")
        if not isinstance(connection, Mapping):
            raise ValueError("Invalid YAML config: 'connection' must be a mapping of connection parameters")

        connection_string = self._create_connection_string_for_config(connection)
        return pyodbc.connect(connection_string)

    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        with connection.cursor() as cursor:
            cursor.execute(sql, params or ())
            if not cursor.description:
                return []

            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def _get_catalogs(self, connection, file_config: Mapping[str, Any]) -> list[str]:
        database = file_config["connection"].get("database")
        if isinstance(database, str) and database:
            return [database]

        rows = self._fetchall_dicts(connection, "SELECT name FROM sys.databases", None)
        all_catalogs = [row["name"] for row in rows]
        return [catalog for catalog in all_catalogs if catalog not in self._IGNORED_CATALOGS]

    def _sql_columns_for_schema(self, catalog: str, schema: str) -> tuple[str, dict | tuple | list | None]:
        sql = f"""
        SELECT table_name, column_name, is_nullable, data_type
        FROM [{catalog}].information_schema.columns WHERE table_schema = ?
        """
        return sql, [schema]

    def _construct_column(self, row: dict[str, Any]) -> DatabaseColumn:
        return DatabaseColumn(
            name=row["column_name"],
            type=row["data_type"],
            nullable=str(row.get("is_nullable", "NO")).upper() == "YES",
        )

    def _sql_list_schemas(self, catalogs: list[str] | None) -> tuple[str, dict | tuple | list | None]:
        if not catalogs:
            return "SELECT schema_name, catalog_name FROM information_schema.schemata", None

        parts = []
        for catalog in catalogs:
            parts.append(f"SELECT schema_name, catalog_name FROM {catalog}.information_schema.schemata")
        return " UNION ALL ".join(parts), None

    def _create_connection_string_for_config(self, file_config: Mapping[str, Any]) -> str:
        driver = file_config.get("driver", "ODBC Driver 17 for SQL Server")
        host = file_config.get("host")
        port = file_config.get("port", 1433)
        instance = file_config.get("instanceName")

        if instance:
            server_part = f"{host}\\{instance}"
        else:
            server_part = f"{host},{port}"

        connection_parts = {
            "driver": f"{{{driver}}}",
            "server": server_part,
            "database": file_config.get("database"),
            "uid": file_config.get("user"),
            "pwd": file_config.get("password", ""),
            "encrypt": file_config.get("encrypt"),
            "TrustServerCertificate": "yes" if file_config.get("trust_server_certificate") else None,
        }

        connection_string = ";".join(f"{k}={v}" for k, v in connection_parts.items() if v is not None)
        return connection_string
