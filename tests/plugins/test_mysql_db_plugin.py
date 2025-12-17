from datetime import datetime
from typing import Any, Mapping

import pymysql
import pytest
from testcontainers.mysql import MySqlContainer  # type: ignore

from nemory.pluginlib.plugin_utils import execute_datasource_plugin
from nemory.plugins.databases.databases_types import DatabaseColumn, DatabaseIntrospectionResult
from nemory.plugins.mysql_db_plugin import MySQLDbPlugin
from tests.plugins.test_database_utils import assert_database_structure


@pytest.fixture
def create_mysql_conn(mysql_container: MySqlContainer):
    def _create_connection():
        return pymysql.connect(
            host=mysql_container.get_container_host_ip(),
            port=int(mysql_container.get_exposed_port(mysql_container.port)),
            user="root",
            password=mysql_container.root_password,
            database=mysql_container.dbname,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )

    return _create_connection


@pytest.fixture(scope="module")
def mysql_container():
    container = MySqlContainer()
    container.start()
    yield container
    container.stop()


def _init_mysql_catalogs(create_mysql_conn, mysql_container: MySqlContainer, with_samples: bool = False):
    conn = create_mysql_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute("DROP DATABASE IF EXISTS catalog_main;")
            cursor.execute("CREATE DATABASE IF NOT EXISTS catalog_main;")
            cursor.execute("DROP SCHEMA IF EXISTS catalog_aux;")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS catalog_aux;")

            cursor.execute(f"GRANT ALL PRIVILEGES ON *.* TO '{mysql_container.username}'@'%';")
            cursor.execute("FLUSH PRIVILEGES;")

            cursor.execute("USE catalog_main;")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS table_users (
                    id INT NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    email VARCHAR(255) NULL
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS table_products (
                    id INT NOT NULL,
                    price DECIMAL(10,2) NOT NULL,
                    description TEXT NULL
                );
            """)
            cursor.execute("USE catalog_aux;")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS table_orders (
                    id INT NOT NULL,
                    user_id INT NOT NULL,
                    created_at TIMESTAMP
                );
            """)

            if with_samples:
                cursor.execute("USE catalog_main;")
                cursor.execute(
                    "INSERT INTO table_users (id, name, email) VALUES (1, 'Alice', 'a@example.com'), (2, 'Bob', NULL);"
                )
                cursor.execute(
                    "INSERT INTO table_products (id, price, description) VALUES (1, 10.50, 'foo'), (2, 20.00, NULL);"
                )
                cursor.execute("USE catalog_aux;")
                cursor.execute(
                    "INSERT INTO table_orders (id, user_id, created_at) VALUES (1, 1, '2025-12-16 12:00:00'), (2, 2, NULL);"
                )
    finally:
        conn.close()


@pytest.mark.parametrize("with_samples", [False, True], ids=["database_structure", "database_structure_with_samples"])
def test_mysql_introspection(mysql_container: MySqlContainer, create_mysql_conn, with_samples):
    _init_mysql_catalogs(create_mysql_conn, mysql_container, with_samples)
    plugin = MySQLDbPlugin()
    config_file = _create_config_file_from_container(mysql_container)
    result = execute_datasource_plugin(plugin, config_file["type"], config_file, "file_name").result

    expected_structure = {
        "default": {
            "catalog_main": {
                "table_users": [
                    DatabaseColumn("id", "int", False),
                    DatabaseColumn("name", "varchar", False),
                    DatabaseColumn("email", "varchar", True),
                ],
                "table_products": [
                    DatabaseColumn("id", "int", False),
                    DatabaseColumn("price", "decimal", False),
                    DatabaseColumn("description", "text", True),
                ],
            },
            "catalog_aux": {
                "table_orders": [
                    DatabaseColumn("id", "int", False),
                    DatabaseColumn("user_id", "int", False),
                    DatabaseColumn("created_at", "timestamp", True),
                ],
            },
            "test": {},
        }
    }
    assert_database_structure(result, expected_structure, with_samples)


def _init_mysql_big_table(create_mysql_conn):
    conn = create_mysql_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute("CREATE DATABASE IF NOT EXISTS custom;")
            cursor.execute("USE custom;")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS test (
                    id INT NOT NULL,
                    name VARCHAR(255) NULL
                );
                """
            )
            values = ", ".join(f"({i}, 'name{i}')" for i in range(100))
            cursor.execute(f"INSERT INTO test (id, name) VALUES {values};")
    finally:
        conn.close()


def test_mysql_exact_samples(mysql_container: MySqlContainer, create_mysql_conn):
    _init_mysql_catalogs(create_mysql_conn, mysql_container, with_samples=True)
    plugin = MySQLDbPlugin()
    config_file = _create_config_file_from_container(mysql_container)
    execution_result = execute_datasource_plugin(plugin, config_file["type"], config_file, "file_name")
    assert isinstance(execution_result.result, DatabaseIntrospectionResult)

    catalogs = {c.name: c for c in execution_result.result.catalogs}
    main_schema = {s.name: s for s in catalogs["default"].schemas}["catalog_main"]
    table_products = {t.name: t for t in main_schema.tables}["table_products"]
    expected_rows_catalog_main = [
        {"id": 1, "price": 10.50, "description": "foo"},
        {"id": 2, "price": 20.00, "description": None},
    ]
    assert table_products.samples == expected_rows_catalog_main

    aux_schema = {s.name: s for s in catalogs["default"].schemas}["catalog_aux"]
    table_orders = {t.name: t for t in aux_schema.tables}["table_orders"]
    expected_rows_catalog_aux = [
        {"id": 1, "user_id": 1, "created_at": datetime(2025, 12, 16, 12, 0)},
        {"id": 2, "user_id": 2, "created_at": None},
    ]
    assert table_orders.samples == expected_rows_catalog_aux


def test_mysql_samples_in_big(mysql_container: MySqlContainer, create_mysql_conn):
    _init_mysql_big_table(create_mysql_conn)
    plugin = MySQLDbPlugin()
    limit = plugin._introspector._SAMPLE_LIMIT
    config_file = _create_config_file_from_container(mysql_container)
    execution_result = execute_datasource_plugin(plugin, config_file["type"], config_file, "file_name")
    assert isinstance(execution_result.result, DatabaseIntrospectionResult)
    catalogs = {c.name: c for c in execution_result.result.catalogs}
    schema = {s.name: s for s in catalogs["default"].schemas}["custom"]
    table = {t.name: t for t in schema.tables}["test"]
    assert len(table.samples) == limit


def _create_config_file_from_container(mysql: MySqlContainer) -> Mapping[str, Any]:
    return {
        "type": "databases/mysql",
        "connection": {
            "host": mysql.get_container_host_ip(),
            "port": int(mysql.get_exposed_port(mysql.port)),
            # TODO now this parameter is not used in introspections, worth checking if that is expected behaviour
            "database": mysql.dbname,
            "user": mysql.username,
            "password": mysql.password,
        },
    }
