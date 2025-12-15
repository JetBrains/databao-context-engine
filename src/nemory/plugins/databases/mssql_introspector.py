from __future__ import annotations

from typing import Any, Mapping

from mssql_python import connect  # type: ignore
from pydantic import Field

from nemory.plugins.base_db_plugin import BaseDatabaseConfigFile
from nemory.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from nemory.plugins.databases.databases_types import DatabaseColumn


class MSSQLConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="databases/mssql")
    connection: dict[str, Any] = Field(
        description="Connection parameters for the Microsoft Server SQL database. It can contain any of the keys supported by the Microsoft Server connection library"
    )


class MSSQLIntrospector(BaseIntrospector[MSSQLConfigFile]):
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

    def _connect(self, file_config: MSSQLConfigFile):
        connection = file_config.connection
        if not isinstance(connection, Mapping):
            raise ValueError("Invalid YAML config: 'connection' must be a mapping of connection parameters")

        connection_string = self._create_connection_string_for_config(connection)
        return connect(connection_string)

    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        with connection.cursor() as cursor:
            cursor.execute(sql, params or ())
            if not cursor.description:
                return []

            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def _get_catalogs(self, connection, file_config: MSSQLConfigFile) -> list[str]:
        database = file_config.connection.get("database")
        if isinstance(database, str) and database:
            return [database]

        rows = self._fetchall_dicts(connection, "SELECT name FROM sys.databases", None)
        all_catalogs = [row["name"] for row in rows]
        return [catalog for catalog in all_catalogs if catalog not in self._IGNORED_CATALOGS]

    def _sql_columns_for_schema(self, catalog: str, schema: str) -> SQLQuery:
        sql = f"""
        SELECT table_name, column_name, is_nullable, data_type
        FROM [{catalog}].information_schema.columns WHERE table_schema = ?
        """
        return SQLQuery(sql, [schema])

    def _construct_column(self, row: dict[str, Any]) -> DatabaseColumn:
        return DatabaseColumn(
            name=row["column_name"],
            type=row["data_type"],
            nullable=str(row.get("is_nullable", "NO")).upper() == "YES",
        )

    def _sql_list_schemas(self, catalogs: list[str] | None) -> SQLQuery:
        if not catalogs:
            return SQLQuery("SELECT schema_name, catalog_name FROM information_schema.schemata", None)

        parts = []
        for catalog in catalogs:
            parts.append(f"SELECT schema_name, catalog_name FROM {catalog}.information_schema.schemata")
        return SQLQuery(" UNION ALL ".join(parts), None)

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

        connection_string = ";".join(f"{k}={v}" for k, v in connection_parts.items() if v is not None)
        return connection_string

    def _sql_sample_rows(self, catalog: str, schema: str, table: str, limit: int) -> SQLQuery:
        sql = f'SELECT TOP ({limit}) * FROM "{schema}"."{table}"'
        return SQLQuery(sql, [limit])
