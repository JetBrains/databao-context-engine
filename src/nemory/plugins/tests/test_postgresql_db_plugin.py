from typing import Any, Mapping

import psycopg
import pytest
from testcontainers.postgres import PostgresContainer  # type: ignore

from nemory.plugins.databases.databases_types import (
    DatabaseCatalog,
    DatabaseColumn,
    DatabaseIntrospectionResult,
    DatabaseSchema,
    DatabaseTable,
)
from nemory.plugins.postgresql_db_plugin import PostgresqlDbPlugin


@pytest.fixture(scope="module")
def postgres_container():
    container = PostgresContainer("postgres:18.0", driver=None)
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="module")
def postgres_container_with_columns(postgres_container):
    connection_url = postgres_container.get_connection_url()
    with psycopg.connect(connection_url) as conn:
        cursor = conn.cursor()

        cursor.execute("""
                CREATE SCHEMA custom;
                CREATE TABLE custom.test (id int not null, name varchar(255) null)
                """)

    return postgres_container


def test_postgres_container(postgres_container_with_columns: PostgresContainer):
    plugin = PostgresqlDbPlugin()

    config_file = _create_config_file_from_container(postgres_container_with_columns)

    execution_result = plugin.execute(config_file["type"], config_file)

    assert execution_result.result == DatabaseIntrospectionResult(
        catalogs=[
            DatabaseCatalog(
                name="test",
                schemas=[
                    DatabaseSchema(
                        name="public",
                        tables=[],
                    ),
                    DatabaseSchema(
                        name="custom",
                        tables=[
                            DatabaseTable(
                                name="test",
                                columns=[
                                    DatabaseColumn(name="id", type="int4", nullable=False),
                                    DatabaseColumn(name="name", type="varchar", nullable=True),
                                ],
                                samples=[],
                            )
                        ],
                    ),
                ],
            )
        ]
    )


def _create_config_file_from_container(postgres_container_with_columns: PostgresContainer) -> Mapping[str, Any]:
    return {
        "type": "databases/postgres",
        "host": postgres_container_with_columns.get_container_host_ip(),
        "port": postgres_container_with_columns.get_exposed_port(postgres_container_with_columns.port),
        "database": postgres_container_with_columns.dbname,
        "user": postgres_container_with_columns.username,
        "password": postgres_container_with_columns.password,
    }
