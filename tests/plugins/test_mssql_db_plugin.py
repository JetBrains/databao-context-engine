import platform
from typing import Any, Mapping

import mssql_python  # type: ignore
import pytest
from testcontainers.mssql import SqlServerContainer  # type: ignore

from nemory.pluginlib.plugin_utils import execute_datasource_plugin
from nemory.plugins.databases.databases_types import DatabaseColumn
from nemory.plugins.mssql_db_plugin import MSSQLDbPlugin
from tests.plugins.test_database_utils import assert_database_structure

MSSQL_HTTP_PORT = 1433
# doesn't work with mssql_container.get_container_host_ip (localhost)
MSSQL_HOST = "127.0.0.1"


def _is_nixos_distro() -> bool:
    try:
        os_release = platform.freedesktop_os_release()
        release_name = os_release["NAME"]
        return "nixos" in release_name.lower()
    except OSError | KeyError:
        return False


@pytest.fixture(scope="module")
def mssql_container():
    if _is_nixos_distro():
        pytest.skip("mssql-python connector doesn't work on NixOS out of the box")

    container = SqlServerContainer()
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="module")
def mssql_container_with_columns(mssql_container: SqlServerContainer):
    port = mssql_container.get_exposed_port(MSSQL_HTTP_PORT)
    connection_parts = {
        "server": f"{MSSQL_HOST},{port}",
        "database": mssql_container.dbname,
        "uid": mssql_container.username,
        "pwd": mssql_container.password,
        "encrypt": "no",
    }
    connection_string = ";".join(f"{k}={v}" for k, v in connection_parts.items() if v is not None)
    conn = mssql_python.connect(
        connection_string,
        autocommit=True,
    )

    with conn.cursor() as cursor:
        cursor.execute("CREATE DATABASE catalog_main;")
        cursor.execute("CREATE DATABASE catalog_aux;")

        cursor.execute("USE catalog_main;")
        cursor.execute("""
            CREATE TABLE table_users (
                id INT NOT NULL,
                name  VARCHAR(255) NOT NULL,
                email VARCHAR(255) NULL
            );
        """)
        cursor.execute("""
        CREATE TABLE table_products (
            id INT NOT NULL,
            price DECIMAL(10, 2) NOT NULL,
            description NVARCHAR(MAX) NULL
        );
        """)

        cursor.execute("USE catalog_aux;")
        cursor.execute("""
        CREATE TABLE table_orders (
            id INT NOT NULL,
            user_id INT NOT NULL,
            created_at DATETIME NULL
        );
        """)
    return mssql_container


def test_mssql_introspection(mssql_container_with_columns):
    plugin = MSSQLDbPlugin()
    config_file = _create_config_file_from_container(mssql_container_with_columns)
    result = execute_datasource_plugin(plugin, config_file["type"], config_file, "file_name").result

    # should guest schema be ignored?
    expected_structure = {
        "catalog_main": {
            "dbo": {
                "table_users": [
                    DatabaseColumn("id", "int", False),
                    DatabaseColumn("name", "varchar", False),
                    DatabaseColumn("email", "varchar", True),
                ],
                "table_products": [
                    DatabaseColumn("id", "int", False),
                    DatabaseColumn("price", "decimal", False),
                    DatabaseColumn("description", "nvarchar", True),
                ],
            },
            "guest": {},
        },
        "catalog_aux": {
            "dbo": {
                "table_orders": [
                    DatabaseColumn("id", "int", False),
                    DatabaseColumn("user_id", "int", False),
                    DatabaseColumn("created_at", "datetime", True),
                ],
            },
            "guest": {},
        },
    }
    assert_database_structure(result, expected_structure)


def _create_config_file_from_container(mssql: SqlServerContainer) -> Mapping[str, Any]:
    return {
        "type": "databases/mssql",
        "connection": {
            "host": MSSQL_HOST,
            "port": int(mssql.get_exposed_port(MSSQL_HTTP_PORT)),
            "user": mssql.username,
            "password": mssql.password,
            "encrypt": "no",
        },
    }
