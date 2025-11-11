from dataclasses import dataclass
from typing import Any

import psycopg2
import pytest
from testcontainers.postgres import PostgresContainer  # type: ignore


@dataclass
class DatabaseColumn:
    name: str
    type: str
    nullable: bool
    description: str | None = None


@dataclass
class DatabaseTable:
    name: str
    columns: list[DatabaseColumn]


@dataclass
class DatabaseSchema:
    name: str
    tables: list[DatabaseTable]


@dataclass
class DatabaseCatalog:
    name: str
    schemas: list[DatabaseSchema]


@dataclass
class IntrospectionResult:
    catalogs: list[DatabaseCatalog]


IGNORED_POSTGRES_SCHEMAS = [
    "information_schema",
    "pg_catalog",
    "pg_toast",
]


class DatabaseCatalogIntrospector:
    def __init__(self, cursor, catalog_name: str, ignored_schemas: list[str]):
        self.cursor = cursor
        self.catalog_name = catalog_name
        self.ignored_schemas = ignored_schemas

    def collect_schemas(self) -> list[str]:
        self.cursor.execute(
            "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = %(catalog)s",
            {"catalog": self.catalog_name},
        )
        return [schema for (schema,) in self.cursor.fetchall()]

    def collect_tables(self, schema: str) -> list[str]:
        self.cursor.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_catalog = %(catalog)s AND table_schema = %(schema)s
            """,
            {"catalog": self.catalog_name, "schema": schema},
        )
        return [table for (table,) in self.cursor.fetchall()]

    def collect_columns(self, schema: str, table: str) -> list[dict[str, Any]]:
        self.cursor.execute(
            """
            SELECT * FROM information_schema.columns
            WHERE table_catalog = %(catalog)s AND table_schema = %(schema)s AND table_name = %(table)s
            """,
            {"catalog": self.catalog_name, "schema": schema, "table": table},
        )
        columns = [col[0] for col in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor.fetchall()]

    def introspect_column(self, column: dict[str, Any]) -> DatabaseColumn:
        column_name = column["column_name"]
        column_type = column["udt_name"]
        is_nullable = "yes" == column["is_nullable"]
        return DatabaseColumn(column_name, column_type, is_nullable)

    def introspect_table(self, schema, table) -> DatabaseTable:
        columns = [self.introspect_column(column) for column in self.collect_columns(schema, table)]
        return DatabaseTable(table, columns)

    def introspect_schema(self, schema: str) -> DatabaseSchema:
        tables = [self.introspect_table(schema, table) for table in self.collect_tables(schema)]
        return DatabaseSchema(schema, tables)

    def introspect(self) -> DatabaseCatalog:
        schemas = [
            self.introspect_schema(schema) for schema in self.collect_schemas() if schema not in self.ignored_schemas
        ]
        return DatabaseCatalog(self.catalog_name, schemas)


def introspect(cursor, dbname) -> IntrospectionResult:
    cursor.execute("SELECT datname FROM pg_catalog.pg_database WHERE datistemplate = false")
    catalog_names = cursor.fetchall()
    assert dbname in [catalog for (catalog,) in catalog_names]
    introspector = DatabaseCatalogIntrospector(cursor, dbname, IGNORED_POSTGRES_SCHEMAS)
    catalog = introspector.introspect()
    return IntrospectionResult([catalog])


@pytest.fixture(scope="module")
def postgres_container():
    container = PostgresContainer("postgres:18.0", driver=None)
    container.start()
    yield container
    container.stop()


def test_postgres_container(postgres_container: PostgresContainer):
    connection_url = postgres_container.get_connection_url()
    conn = psycopg2.connect(connection_url)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE SCHEMA custom;
        CREATE TABLE custom.test (id int not null, name varchar(255) null)
        """)
    introspection_result = introspect(cursor, postgres_container.dbname)

    assert introspection_result == IntrospectionResult(
        [
            DatabaseCatalog(
                "test",
                [
                    DatabaseSchema(
                        name="public",
                        tables=[],
                    ),
                    DatabaseSchema(
                        "custom",
                        tables=[
                            DatabaseTable(
                                "test",
                                [
                                    DatabaseColumn("id", "int4", False),
                                    DatabaseColumn("name", "varchar", False),
                                ],
                            )
                        ],
                    ),
                ],
            )
        ]
    )
    print(introspection_result)
