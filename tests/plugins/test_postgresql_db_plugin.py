from datetime import datetime
from typing import Any, Mapping

import psycopg
import pytest
from pytest_unordered import unordered
from testcontainers.postgres import PostgresContainer  # type: ignore

from nemory.pluginlib.build_plugin import BuildExecutionResult, EmbeddableChunk
from nemory.plugins.databases.database_chunker import DatabaseColumnChunkContent, DatabaseTableChunkContent
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


def test_postgres_plugin_execute(postgres_container_with_columns: PostgresContainer):
    plugin = PostgresqlDbPlugin()

    config_file = _create_config_file_from_container(postgres_container_with_columns)

    execution_result = plugin.execute(config_file["type"], "file_name", config_file)

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


def test_postgres_plugin_divide_into_chunks():
    plugin = PostgresqlDbPlugin()

    input = BuildExecutionResult(
        id="id",
        name="name",
        type="databases/postgres",
        description=None,
        version=None,
        executed_at=datetime.now(),
        result=DatabaseIntrospectionResult(
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
                                    description="best table",
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
        ),
    )

    chunks = plugin.divide_result_into_chunks(input)

    assert len(chunks) == 3
    assert chunks == unordered(
        EmbeddableChunk(
            embeddable_text="test - best table",
            content=DatabaseTableChunkContent(
                catalog_name="test",
                schema_name="custom",
                table=DatabaseTable(
                    name="test",
                    description="best table",
                    columns=[
                        DatabaseColumn(name="id", type="int4", nullable=False),
                        DatabaseColumn(name="name", type="varchar", nullable=True),
                    ],
                    samples=[],
                ),
            ),
        ),
        EmbeddableChunk(
            embeddable_text="id",
            content=DatabaseColumnChunkContent(
                catalog_name="test",
                schema_name="custom",
                table_name="test",
                column=DatabaseColumn(name="id", type="int4", nullable=False),
            ),
        ),
        EmbeddableChunk(
            embeddable_text="name",
            content=DatabaseColumnChunkContent(
                catalog_name="test",
                schema_name="custom",
                table_name="test",
                column=DatabaseColumn(name="name", type="varchar", nullable=True),
            ),
        ),
    )


def _create_config_file_from_container(postgres_container_with_columns: PostgresContainer) -> Mapping[str, Any]:
    return {
        "type": "databases/postgres",
        "connection": {
            "host": postgres_container_with_columns.get_container_host_ip(),
            "port": postgres_container_with_columns.get_exposed_port(postgres_container_with_columns.port),
            "database": postgres_container_with_columns.dbname,
            "user": postgres_container_with_columns.username,
            "password": postgres_container_with_columns.password,
        },
    }
