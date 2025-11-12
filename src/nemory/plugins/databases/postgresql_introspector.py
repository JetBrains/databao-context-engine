from collections import defaultdict
from typing import Any, Mapping

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row

from nemory.plugins.databases.databases_types import (
    DatabaseCatalog,
    DatabaseColumn,
    DatabaseIntrospectionResult,
    DatabaseSchema,
    DatabaseTable,
)


class PostgresqlIntrospector:
    _IGNORED_SCHEMAS = ["information_schema", "pg_catalog", "pg_toast"]

    def introspect_database(self, file_config: Mapping[str, Any]) -> DatabaseIntrospectionResult:
        connection_string = self._create_connection_string_for_config(file_config)

        connection = psycopg.connect(connection_string)
        catalogs_names = self._get_catalog_to_introspect(file_config, connection)
        all_schemas_per_catalog = self._collect_schemas(connection, catalogs_names)

        introspected_catalogs: list[DatabaseCatalog] = []
        for catalog in all_schemas_per_catalog:
            schemas = []
            for schema in all_schemas_per_catalog[catalog]:
                columns_per_table = self._collect_columns_for_schema(connection, catalog, schema)

                schemas.append(
                    DatabaseSchema(
                        name=schema,
                        tables=[
                            DatabaseTable(
                                name=table,
                                columns=columns_per_table[table],
                                samples=[],  # TODO: fill samples
                            )
                            for table in columns_per_table
                        ],
                    )
                )
            introspected_catalogs.append(DatabaseCatalog(name=catalog, schemas=schemas))

        return DatabaseIntrospectionResult(catalogs=introspected_catalogs)

    def _create_connection_string_for_config(self, file_config: Mapping[str, Any]) -> str:
        # TODO: For all fields, surround with single quotes and escape backslasshes and quotes in the values
        #  (or use a different connection method)
        host = file_config.get("host")
        if host is None:
            raise ValueError("A host must be provided to connect to the PostgreSQL database.")

        port = file_config.get("port", 5432)

        connection_string = f"host={host} port={port}"

        database = file_config.get("database")
        if database is not None:
            connection_string += f" dbname={database}"

        user = file_config.get("user")
        if user is not None:
            connection_string += f" user={user}"

        password = file_config.get("password")
        if password is not None:
            connection_string += f" password='{password}'"

        return connection_string

    def _get_catalog_to_introspect(self, file_config: Mapping[str, Any], connection: Connection) -> list[str]:
        database = file_config.get("database")
        if database is not None:
            return [database]

        catalog_results = connection.execute(
            "SELECT datname FROM pg_catalog.pg_database WHERE datistemplate = false"
        ).fetchall()

        return [row[0] for row in catalog_results]

    def _collect_schemas(self, connection: Connection, catalogs: list[str]) -> dict[str, list[str]]:
        schemas_result = connection.execute(
            "SELECT catalog_name, schema_name FROM information_schema.schemata WHERE catalog_name = ANY(%s) AND schema_name != ALL(%s)",
            (catalogs, self._IGNORED_SCHEMAS),
        ).fetchall()

        schemas_per_catalog = defaultdict(list)
        for row in schemas_result:
            schemas_per_catalog[row[0]].append(row[1])

        return schemas_per_catalog

    def _collect_tables(self, connection: Connection, catalogs: list[str]) -> dict[str, dict[str, list[str]]]:
        tables_result = connection.execute(
            "SELECT table_catalog, table_schema, table_name FROM information_schema.tables WHERE table_catalog = ANY(%s)",
            [catalogs],
        ).fetchall()

        tables_per_schema_per_catalog: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
        for row in tables_result:
            tables_per_schema_per_catalog[row[0]][row[1]].append(row[2])

        return tables_per_schema_per_catalog

    def _collect_columns_for_schema(
        self, connection: Connection, catalog: str, schema: str
    ) -> dict[str, list[DatabaseColumn]]:
        columns_result = (
            connection.cursor(row_factory=dict_row)
            .execute(
                "SELECT * FROM information_schema.columns WHERE table_catalog = %s AND table_schema = %s",
                [catalog, schema],
            )
            .fetchall()
        )

        columns_per_table = defaultdict(list)
        for row in columns_result:
            columns_per_table[row["table_name"]].append(self._convert_result_row_to_database_column(row))

        return columns_per_table

    def _convert_result_row_to_database_column(self, row) -> DatabaseColumn:
        return DatabaseColumn(
            name=row["column_name"],
            type=row["udt_name"],
            nullable=row["is_nullable"] == "YES",
        )
