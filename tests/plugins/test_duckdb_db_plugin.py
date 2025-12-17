from pathlib import Path
from typing import Mapping, Any

import duckdb
import pytest

from nemory.pluginlib.plugin_utils import execute_datasource_plugin
from nemory.plugins.databases.databases_types import (
    DatabaseColumn,
)
from nemory.plugins.duckdb_db_plugin import DuckDbPlugin
from tests.plugins.test_database_utils import assert_database_structure


@pytest.fixture
def temp_duckdb_file(tmp_path: Path):
    db_file = tmp_path / "test_db.duckdb"
    yield db_file


def execute_duckdb_queries(db_file: Path, *queries: str):
    conn = duckdb.connect(database=str(db_file))
    with conn:
        for query in queries:
            conn.execute(query)


@pytest.fixture
def duckdb_with_custom_schema(temp_duckdb_file: Path):
    execute_duckdb_queries(
        temp_duckdb_file,
        "CREATE SCHEMA custom",
        "CREATE TABLE custom.test (id INTEGER NOT NULL, name VARCHAR)",
        "CREATE TYPE test_enum AS ENUM ('a', 'b')",
        "CREATE TABLE custom.test2 (id FLOAT, name test_enum)",
        "CREATE SCHEMA another",
    )
    return temp_duckdb_file


@pytest.mark.parametrize("with_samples", [False, True], ids=["database_structure", "database_structure_with_samples"])
def test_duckdb_plugin_introspection(temp_duckdb_file: Path, with_samples):
    execute_duckdb_queries(temp_duckdb_file, "CREATE TABLE test (id INTEGER NOT NULL)")
    if with_samples:
        execute_duckdb_queries(temp_duckdb_file, "INSERT INTO test (id) VALUES (1), (2), (3)")
    plugin = DuckDbPlugin()
    config = _create_config_file_from_container(temp_duckdb_file)
    result = execute_datasource_plugin(plugin, config["type"], config, "file_name").result

    expected_structure = {
        "test_db": {
            "main": {
                "test": [
                    DatabaseColumn("id", "INTEGER", False),
                ],
            }
        }
    }
    assert_database_structure(result, expected_structure, with_samples)


@pytest.mark.parametrize("with_samples", [False, True], ids=["database_structure", "database_structure_with_samples"])
def test_duckdb_plugin_introspection_custom_schema(duckdb_with_custom_schema: Path, with_samples):
    if with_samples:
        execute_duckdb_queries(
            duckdb_with_custom_schema,
            "INSERT INTO custom.test (id, name) VALUES (1, 'Alice'), (2, NULL)",
            "INSERT INTO custom.test2 (id, name) VALUES (1.5, 'a'), (NULL, 'b')",
        )
    plugin = DuckDbPlugin()
    config = _create_config_file_from_container(duckdb_with_custom_schema)
    result = execute_datasource_plugin(plugin, config["type"], config, "file_name").result

    expected_structure = {
        "test_db": {
            "main": {},
            "custom": {
                "test": [
                    DatabaseColumn("id", "INTEGER", False),
                    DatabaseColumn("name", "VARCHAR", True),
                ],
                "test2": [
                    DatabaseColumn("id", "FLOAT", True),
                    DatabaseColumn("name", "ENUM('a', 'b')", True),
                ],
            },
            "another": {},
        }
    }
    assert_database_structure(result, expected_structure, with_samples)


def test_duckdb_exact_samples(temp_duckdb_file: Path):
    execute_duckdb_queries(temp_duckdb_file, "CREATE TABLE test (id INTEGER NOT NULL)")
    execute_duckdb_queries(temp_duckdb_file, "INSERT INTO test (id) VALUES (1), (2), (3)")
    plugin = DuckDbPlugin()
    config = _create_config_file_from_container(temp_duckdb_file)
    result = execute_datasource_plugin(plugin, config["type"], config, "file_name").result
    catalogs = {c.name: c for c in result.catalogs}
    main_schema = {s.name: s for s in catalogs["test_db"].schemas}["main"]
    table = {t.name: t for t in main_schema.tables}["test"]
    assert table.samples == [{"id": 1}, {"id": 2}, {"id": 3}]


def test_duckdb_samples_in_big(temp_duckdb_file: Path):
    execute_duckdb_queries(
        temp_duckdb_file,
        "CREATE TABLE test_big (id INTEGER NOT NULL, name VARCHAR)",
        "INSERT INTO test_big (id, name) VALUES " + ", ".join(f"({i}, 'name{i}')" for i in range(100)),
    )
    plugin = DuckDbPlugin()
    limit = plugin._introspector._SAMPLE_LIMIT
    config = _create_config_file_from_container(temp_duckdb_file)
    result = execute_datasource_plugin(plugin, config["type"], config, "file_name").result
    catalogs = {c.name: c for c in result.catalogs}
    main_schema = {s.name: s for s in catalogs["test_db"].schemas}["main"]
    table = {t.name: t for t in main_schema.tables}["test_big"]
    assert len(table.samples) == limit


def _create_config_file_from_container(duckdb_path: Path) -> Mapping[str, Any]:
    return {
        "type": "databases/duckdb",
        "connection": dict(database=str(duckdb_path)),
    }
