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
from tests.plugins.database_test_utils import assert_database_structure

HTTP_PORT = 8123


@pytest.fixture(scope="module")
def clickhouse_container():
    container = ClickHouseContainer()
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="module")
def clickhouse_container_with_columns(clickhouse_container: ClickHouseContainer):
    client = clickhouse_connect.get_client(
        host=clickhouse_container.get_container_host_ip(),
        port=int(clickhouse_container.get_exposed_port(HTTP_PORT)),
        username=clickhouse_container.username,
        password=clickhouse_container.password,
        database=clickhouse_container.dbname,
    )
    try:
        client.command("CREATE DATABASE IF NOT EXISTS custom")
        client.command(
            "CREATE TABLE IF NOT EXISTS custom.test (id Int32 NOT NULL, name Nullable(String)) ENGINE = Memory"
        )
    finally:
        client.close()

    return clickhouse_container


def test_clickhouse_plugin_execute(clickhouse_container_with_columns: ClickHouseContainer):
    plugin = ClickhouseDbPlugin()
    config_file = _create_config_file_from_container(clickhouse_container_with_columns)
    execution_result = execute_datasource_plugin(plugin, config_file["type"], config_file, "file_name")
    assert isinstance(execution_result.result, DatabaseIntrospectionResult)
    expected = {
        "default": {
            clickhouse_container_with_columns.dbname: {},
            "default": {},
            "custom": {
                "test": [
                    DatabaseColumn(name="id", type="Int32", nullable=False),
                    DatabaseColumn(name="name", type="String", nullable=True),
                ]
            },
        }
    }

    assert_database_structure(execution_result.result, expected)


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
