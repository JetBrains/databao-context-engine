import contextlib
from typing import Any, Mapping

import clickhouse_connect
import pytest
from testcontainers.clickhouse import ClickHouseContainer  # type: ignore

from nemory.pluginlib.plugin_utils import execute_datasource_plugin
from nemory.plugins.clickhouse_db_plugin import ClickhouseDbPlugin
from nemory.plugins.databases.databases_types import (
    DatabaseColumn,
    DatabaseIntrospectionResult,
)
from tests.plugins.test_database_utils import assert_database_structure

HTTP_PORT = 8123


@pytest.fixture(scope="module")
def clickhouse_container():
    container = ClickHouseContainer()
    container.start()
    yield container
    container.stop()


@pytest.fixture
def create_clickhouse_client(clickhouse_container: ClickHouseContainer):
    def _create_client(database: str | None = None):
        return clickhouse_connect.get_client(
            host=clickhouse_container.get_container_host_ip(),
            port=int(clickhouse_container.get_exposed_port(HTTP_PORT)),
            username=clickhouse_container.username,
            password=clickhouse_container.password,
            database=database or clickhouse_container.dbname,
        )

    return _create_client


@pytest.fixture
def create_clickhouse_db(create_clickhouse_client, request):
    @contextlib.contextmanager
    def _create_db(desired_db_name: str | None = None):
        db_name = desired_db_name or request.function.__name__
        client = create_clickhouse_client()
        client.command(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        try:
            yield db_name
        finally:
            client.command(f"DROP DATABASE IF EXISTS {db_name}")
            client.close()

    return _create_db


def _init_with_test_table(create_clickhouse_client, db_name: str, with_samples: bool = False):
    client = create_clickhouse_client(database=db_name)
    try:
        client.command("CREATE TABLE test (id Int32 NOT NULL, name Nullable(String)) ENGINE = Memory")
        if with_samples:
            client.command("INSERT INTO test (id, name) VALUES (1, 'Andrew'), (2, 'Boris')")
    finally:
        client.close()


def _init_with_big_table(create_clickhouse_client, db_name: str):
    client = create_clickhouse_client(database=db_name)
    try:
        client.command("CREATE TABLE test (id Int32 NOT NULL, name Nullable(String)) ENGINE = Memory")

        rows, n = [], 1000
        for i in range(n):
            random_name = f"name{i}"
            rows.append((i, random_name))
        values = ", ".join(f"({i}, '{name}')" for i, name in rows)
        client.command(f"INSERT INTO test (id, name) VALUES {values}")
    finally:
        client.close()


@pytest.mark.parametrize("with_samples", [False, True], ids=["database_structure", "database_structure_with_samples"])
def test_clickhouse_plugin_execute(
    create_clickhouse_db, create_clickhouse_client, clickhouse_container: ClickHouseContainer, with_samples
):
    db_name = "custom"
    with create_clickhouse_db(db_name):
        _init_with_test_table(create_clickhouse_client, db_name, with_samples)
        plugin = ClickhouseDbPlugin()
        config_file = _create_config_file_from_container(clickhouse_container)
        execution_result = execute_datasource_plugin(plugin, config_file["type"], config_file, "file_name")
        assert isinstance(execution_result.result, DatabaseIntrospectionResult)
        expected = {
            "default": {
                clickhouse_container.dbname: {},
                "default": {},
                "custom": {
                    "test": [
                        DatabaseColumn(name="id", type="Int32", nullable=False),
                        DatabaseColumn(name="name", type="String", nullable=True),
                    ]
                },
            }
        }
    assert_database_structure(execution_result.result, expected, with_samples)


def test_clickhouse_exact_samples(
    create_clickhouse_db, create_clickhouse_client, clickhouse_container: ClickHouseContainer
):
    db_name = "custom"
    with create_clickhouse_db(db_name):
        _init_with_test_table(create_clickhouse_client, db_name, with_samples=True)
        plugin = ClickhouseDbPlugin()
        config_file = _create_config_file_from_container(clickhouse_container)
        execution_result = execute_datasource_plugin(plugin, config_file["type"], config_file, "file_name")
        assert isinstance(execution_result.result, DatabaseIntrospectionResult)
        catalogs = {c.name: c for c in execution_result.result.catalogs}
        schema = {s.name: s for s in catalogs["default"].schemas}["custom"]
        table = {t.name: t for t in schema.tables}["test"]
        samples = table.samples
        expected_rows = [
            {"id": 1, "name": "Andrew"},
            {"id": 2, "name": "Boris"},
        ]
        assert samples == expected_rows


def test_clickhouse_samples_in_big(
    create_clickhouse_db, create_clickhouse_client, clickhouse_container: ClickHouseContainer
):
    db_name = "custom"
    with create_clickhouse_db(db_name):
        _init_with_big_table(create_clickhouse_client, db_name)
        plugin = ClickhouseDbPlugin()
        limit = plugin._introspector._SAMPLE_LIMIT
        config_file = _create_config_file_from_container(clickhouse_container)
        execution_result = execute_datasource_plugin(plugin, config_file["type"], config_file, "file_name")
        assert isinstance(execution_result.result, DatabaseIntrospectionResult)
        catalogs = {c.name: c for c in execution_result.result.catalogs}
        schema = {s.name: s for s in catalogs["default"].schemas}["custom"]
        table = {t.name: t for t in schema.tables}["test"]
        assert len(table.samples) == limit


def _create_config_file_from_container(clickhouse: ClickHouseContainer) -> Mapping[str, Any]:
    return {
        "type": "databases/clickhouse",
        "connection": {
            "host": clickhouse.get_container_host_ip(),
            "port": int(clickhouse.get_exposed_port(HTTP_PORT)),
            # TODO now this parameter is not used in introspections, worth checking if that is expected behaviour
            "database": clickhouse.dbname,
            "username": clickhouse.username,
            "password": clickhouse.password,
        },
    }
