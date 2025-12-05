from typing import Any, Mapping

import pymysql
import pytest
from testcontainers.mysql import MySqlContainer  # type: ignore

from nemory.pluginlib.plugin_utils import execute_datasource_plugin
from nemory.plugins.databases.databases_types import DatabaseColumn
from nemory.plugins.mysql_db_plugin import MySQLDbPlugin
from tests.plugins.database_test_utils import assert_database_structure


@pytest.fixture(scope="module")
def mysql_container():
    container = MySqlContainer()
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="module")
def mysql_container_with_columns(mysql_container: MySqlContainer):
    conn = pymysql.connect(
        host=mysql_container.get_container_host_ip(),
        port=int(mysql_container.get_exposed_port(mysql_container.port)),
        user="root",
        password=mysql_container.root_password,
        database=mysql_container.dbname,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute("CREATE DATABASE catalog_main;")
            cursor.execute("CREATE SCHEMA catalog_aux;")

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
    finally:
        conn.close()
    return mysql_container


def test_mysql_introspection(mysql_container_with_columns):
    plugin = MySQLDbPlugin()
    config_file = _create_config_file_from_container(mysql_container_with_columns)
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
    assert_database_structure(result, expected_structure)


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
