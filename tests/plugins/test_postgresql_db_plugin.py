import contextlib
import random
import string
from datetime import datetime
from typing import Any, Mapping

import psycopg
import pytest
from pytest_unordered import unordered
from testcontainers.postgres import PostgresContainer  # type: ignore

from nemory.pluginlib.build_plugin import BuildExecutionResult, EmbeddableChunk
from nemory.pluginlib.plugin_utils import execute_datasource_plugin
from nemory.plugins.databases.database_chunker import DatabaseColumnChunkContent, DatabaseTableChunkContent
from nemory.plugins.databases.databases_types import (
    DatabaseCatalog,
    DatabaseColumn,
    DatabaseIntrospectionResult,
    DatabasePartitionInfo,
    DatabaseSchema,
    DatabaseTable,
)
from nemory.plugins.postgresql_db_plugin import PostgresqlDbPlugin
from tests.plugins.test_database_utils import assert_database_structure


@pytest.fixture(scope="module")
def postgres_container():
    container = PostgresContainer("postgres:18.0", driver=None)
    container.start()
    yield container
    container.stop()


@pytest.fixture
def create_db_schema(create_pg_conn, request):
    @contextlib.contextmanager
    def _create_db_schema(desired_schema_name: str | None = None):
        actual_schema_name = desired_schema_name or request.function.__name__
        conn = create_pg_conn()
        conn.execute(f"CREATE SCHEMA {actual_schema_name};")
        conn.commit()
        conn.close()
        try:
            yield actual_schema_name
        finally:
            new_conn = create_pg_conn()
            new_conn.execute(f"DROP SCHEMA IF EXISTS {actual_schema_name} CASCADE;")
            new_conn.commit()
            new_conn.close()

    return _create_db_schema


@pytest.fixture
def create_pg_conn(postgres_container: PostgresContainer):
    def create_connection():
        connection_url = postgres_container.get_connection_url()
        conn = psycopg.connect(connection_url)
        return conn

    yield create_connection


def _init_with_test_table(create_pg_conn, schema_name, with_samples=False):
    with create_pg_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(f"CREATE TABLE {schema_name}.test (id int not null, name varchar(255) null)")

        if with_samples:
            cursor.execute(f"INSERT INTO {schema_name}.test (id, name) VALUES (1, 'Alice'), (2, 'Bob');")


def _init_with_big_table(create_pg_conn, schema_name):
    with create_pg_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(f"CREATE TABLE {schema_name}.test (id int not null, name varchar(255) null)")

        rows, n = [], 1000
        for i in range(n):
            random_name = "".join(random.choices(string.ascii_letters, k=5))
            rows.append((i, random_name))
        cursor.executemany(f"INSERT INTO {schema_name}.test (id, name) VALUES (%s, %s)", rows)


@pytest.mark.parametrize("with_samples", [False, True], ids=["database_structure", "database_structure_with_samples"])
def test_postgres_plugin_execute(create_db_schema, create_pg_conn, postgres_container: PostgresContainer, with_samples):
    schema_name = "custom"
    with create_db_schema(schema_name):
        _init_with_test_table(create_pg_conn, schema_name, with_samples)
        plugin = PostgresqlDbPlugin()

        config_file = _create_config_file_from_container(postgres_container)

        execution_result = execute_datasource_plugin(plugin, config_file["type"], config_file, "file_name")
        expected_catalogs = {
            "test": {
                "public": {},
                "custom": {
                    "test": [
                        DatabaseColumn(name="id", type="int4", nullable=False),
                        DatabaseColumn(name="name", type="varchar", nullable=True),
                    ]
                },
            }
        }
        assert_database_structure(execution_result.result, expected_catalogs, with_samples)


def test_postgres_exact_samples(create_db_schema, create_pg_conn, postgres_container: PostgresContainer):
    schema_name = "custom"
    with create_db_schema(schema_name):
        _init_with_test_table(create_pg_conn, schema_name, with_samples=True)
        plugin = PostgresqlDbPlugin()
        config_file = _create_config_file_from_container(postgres_container)
        execution_result = execute_datasource_plugin(plugin, config_file["type"], config_file, "file_name")
        assert isinstance(execution_result.result, DatabaseIntrospectionResult)
        catalogs = {c.name: c for c in execution_result.result.catalogs}
        schema = {s.name: s for s in catalogs["test"].schemas}[schema_name]
        table = {t.name: t for t in schema.tables}["test"]
        samples = table.samples

        expected_rows = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        assert samples == expected_rows


def test_postgres_samples_in_big(create_db_schema, create_pg_conn, postgres_container: PostgresContainer):
    schema_name = "custom"
    with create_db_schema(schema_name):
        _init_with_big_table(create_pg_conn, schema_name)
        plugin = PostgresqlDbPlugin()
        limit = plugin._introspector._SAMPLE_LIMIT
        config_file = _create_config_file_from_container(postgres_container)
        execution_result = execute_datasource_plugin(plugin, config_file["type"], config_file, "file_name")
        assert isinstance(execution_result.result, DatabaseIntrospectionResult)
        catalogs = {c.name: c for c in execution_result.result.catalogs}
        schema = {s.name: s for s in catalogs["test"].schemas}[schema_name]
        table = {t.name: t for t in schema.tables}["test"]
        print(table.samples)
        assert len(table.samples) == limit


def test_postgres_tables_with_indexes(create_db_schema, create_pg_conn, postgres_container: PostgresContainer):
    schema_name = "custom"
    with create_db_schema(schema_name):
        with create_pg_conn() as conn:
            conn.execute(
                f"""
                CREATE TABLE {schema_name}.indexed_table (
                    id INT NOT NULL,
                    name VARCHAR(255),
                    email VARCHAR(255)
                );

                CREATE UNIQUE INDEX idx_indexed_table_id
                    ON {schema_name}.indexed_table (id);

                CREATE INDEX idx_indexed_table_name
                    ON {schema_name}.indexed_table (name);

                CREATE INDEX idx_indexed_table_name_email
                    ON {schema_name}.indexed_table (name, email);

                INSERT INTO {schema_name}.indexed_table (id, name, email)
                VALUES
                    (1, 'Alice', 'alice@example.com'),
                    (2, 'Bob', 'bob@example.com');
                """
            )

        plugin = PostgresqlDbPlugin()
        config_file = _create_config_file_from_container(postgres_container)
        execution_result = execute_datasource_plugin(plugin, config_file["type"], config_file, "file_name")
        expected_catalogs = {
            "test": {
                "public": {},
                "custom": {
                    "indexed_table": [
                        DatabaseColumn(name="id", type="int4", nullable=False),
                        DatabaseColumn(name="name", type="varchar", nullable=True),
                        DatabaseColumn(name="email", type="varchar", nullable=True),
                    ]
                },
            }
        }

        assert_database_structure(
            execution_result.result,
            expected_catalogs,
            with_samples=True,
        )


def test_postgres_partitions(create_pg_conn, create_db_schema, postgres_container):
    with create_db_schema() as db_schema:
        with create_pg_conn() as conn:
            conn.execute(f"""
                        CREATE TABLE {db_schema}.test_partitions (id int not null, name varchar(255) null)
                        PARTITION BY RANGE (id);

                        CREATE TABLE {db_schema}.test_partitions_1 PARTITION OF {db_schema}.test_partitions
                        FOR VALUES FROM (0) TO (10);

                        CREATE TABLE {db_schema}.test_partitions_2 PARTITION OF {db_schema}.test_partitions
                        FOR VALUES FROM (10) TO (20);
                    """)

        plugin = PostgresqlDbPlugin()

        config_file = _create_config_file_from_container(postgres_container)

        execution_result = execute_datasource_plugin(plugin, config_file["type"], config_file, "file_name")

        assert execution_result.result == DatabaseIntrospectionResult(
            [
                DatabaseCatalog(
                    "test",
                    [
                        DatabaseSchema(
                            name="public",
                            tables=[],
                        ),
                        DatabaseSchema(
                            db_schema,
                            tables=[
                                DatabaseTable(
                                    "test_partitions",
                                    [
                                        DatabaseColumn("id", "int4", False),
                                        DatabaseColumn("name", "varchar", True),
                                    ],
                                    [],
                                    partition_info=DatabasePartitionInfo(
                                        meta={
                                            "columns_in_partition_key": ["id"],
                                            "partitioning_strategy": "range partitioned",
                                        },
                                        partition_tables=[
                                            "test_partitions_1",
                                            "test_partitions_2",
                                        ],
                                    ),
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
            embeddable_text="Table test with columns id,name",
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
            embeddable_text="Column id in table test",
            content=DatabaseColumnChunkContent(
                catalog_name="test",
                schema_name="custom",
                table_name="test",
                column=DatabaseColumn(name="id", type="int4", nullable=False),
            ),
        ),
        EmbeddableChunk(
            embeddable_text="Column name in table test",
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
