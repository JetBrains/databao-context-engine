import platform
from typing import Any, Mapping

import mssql_python  # type: ignore
import pytest
from testcontainers.mssql import SqlServerContainer  # type: ignore

from nemory.pluginlib.plugin_utils import execute_datasource_plugin
from nemory.plugins.databases.databases_types import DatabaseColumn, DatabaseIntrospectionResult
from nemory.plugins.mssql_db_plugin import MSSQLDbPlugin
from nemory.pluginlib.build_plugin import DatasourceType
from tests.plugins.test_database_utils import assert_database_structure

MSSQL_HTTP_PORT = 1433
# doesn't work with mssql_container.get_container_host_ip (localhost)
MSSQL_HOST = "127.0.0.1"


def _is_nixos_distro() -> bool:
    try:
        os_release = platform.freedesktop_os_release()
        release_name = os_release["NAME"]
        return "nixos" in release_name.lower()
    except (OSError, KeyError):
        return False


@pytest.fixture(scope="module")
def mssql_container():
    if _is_nixos_distro():
        pytest.skip("mssql-python connector doesn't work on NixOS out of the box")

    container = SqlServerContainer()
    container.start()
    yield container
    container.stop()


@pytest.fixture
def create_mssql_conn(mssql_container: SqlServerContainer):
    def _create_connection():
        port = mssql_container.get_exposed_port(MSSQL_HTTP_PORT)
        connection_parts = {
            "server": f"{MSSQL_HOST},{port}",
            "database": mssql_container.dbname,
            "uid": mssql_container.username,
            "pwd": mssql_container.password,
            "encrypt": "no",
        }
        connection_string = ";".join(f"{k}={v}" for k, v in connection_parts.items() if v is not None)
        return mssql_python.connect(connection_string, autocommit=True)

    return _create_connection


def _init_mssql_catalogs(create_mssql_conn, with_samples: bool = False):
    conn = create_mssql_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute("DROP DATABASE IF EXISTS catalog_main;")
            cursor.execute("CREATE DATABASE catalog_main;")
            cursor.execute("DROP DATABASE IF EXISTS catalog_aux;")
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
            cursor.execute(
                """
                CREATE TABLE table_orders (
                    id INT NOT NULL,
                    user_id INT NOT NULL
                );
                """
            )

            if with_samples:
                cursor.execute("USE catalog_main;")
                cursor.execute(
                    "INSERT INTO table_users (id, name, email) VALUES (1, 'Alice', 'a@example.com'), (2, 'Bob', NULL);"
                )
                cursor.execute(
                    "INSERT INTO table_products (id, price, description) VALUES (1, 10.50, 'foo'), (2, 20.00, NULL);"
                )
                cursor.execute("USE catalog_aux;")
                cursor.execute("INSERT INTO table_orders (id, user_id) VALUES (1, 1), (2, 2);")
    finally:
        conn.close()


def _init_mssql_big_table(create_mssql_conn):
    conn = create_mssql_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute("CREATE DATABASE custom;")
            cursor.execute("USE custom;")
            cursor.execute(
                """
                CREATE TABLE test (
                    id INT NOT NULL,
                    name VARCHAR(255) NULL
                );
                """
            )
            values = ", ".join(f"({i}, 'name{i}')" for i in range(100))
            cursor.execute(f"INSERT INTO test (id, name) VALUES {values};")
    finally:
        conn.close()


@pytest.mark.parametrize("with_samples", [False, True], ids=["database_structure", "database_structure_with_samples"])
def test_mssql_introspection(mssql_container, create_mssql_conn, with_samples):
    _init_mssql_catalogs(create_mssql_conn, with_samples)
    plugin = MSSQLDbPlugin()
    config_file = _create_config_file_from_container(mssql_container)
    result = execute_datasource_plugin(
        plugin, DatasourceType(full_type=config_file["type"]), config_file, "file_name"
    ).result

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
                ],
            },
            "guest": {},
        },
    }
    assert_database_structure(result, expected_structure, with_samples)


def test_mssql_exact_samples(mssql_container: SqlServerContainer, create_mssql_conn):
    _init_mssql_catalogs(create_mssql_conn, with_samples=True)
    plugin = MSSQLDbPlugin()
    config_file = _create_config_file_from_container(mssql_container)
    execution_result = execute_datasource_plugin(
        plugin, DatasourceType(full_type=config_file["type"]), config_file, "file_name"
    )
    assert isinstance(execution_result.result, DatabaseIntrospectionResult)

    catalogs = {c.name: c for c in execution_result.result.catalogs}
    main_schema = {s.name: s for s in catalogs["catalog_main"].schemas}["dbo"]
    table_products = {t.name: t for t in main_schema.tables}["table_products"]
    expected_rows_catalog_main = [
        {"id": 1, "price": 10.50, "description": "foo"},
        {"id": 2, "price": 20.00, "description": None},
    ]
    assert table_products.samples == expected_rows_catalog_main

    aux_schema = {s.name: s for s in catalogs["catalog_aux"].schemas}["dbo"]
    table_orders = {t.name: t for t in aux_schema.tables}["table_orders"]
    expected_rows_catalog_aux = [{"id": 1, "user_id": 1}, {"id": 2, "user_id": 2}]
    assert table_orders.samples == expected_rows_catalog_aux


def test_mssql_samples_in_big(mssql_container: SqlServerContainer, create_mssql_conn):
    _init_mssql_big_table(create_mssql_conn)
    plugin = MSSQLDbPlugin()
    limit = plugin._introspector._SAMPLE_LIMIT
    config_file = _create_config_file_from_container(mssql_container)
    execution_result = execute_datasource_plugin(
        plugin, DatasourceType(full_type=config_file["type"]), config_file, "file_name"
    )
    assert isinstance(execution_result.result, DatabaseIntrospectionResult)
    catalogs = {c.name: c for c in execution_result.result.catalogs}
    schema = {s.name: s for s in catalogs["custom"].schemas}["dbo"]
    table = {t.name: t for t in schema.tables}["test"]
    assert len(table.samples) == limit


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
