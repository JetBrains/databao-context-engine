from typing import Any, Mapping

from nemory.plugins.databases.databases_types import DatabaseIntrospectionResult, DatabaseColumn

TableSpec = dict[str, list[DatabaseColumn]]  # table_name -> list of DatabaseColumn
SchemaSpec = dict[str, TableSpec]  # schema_name -> TableSpec
CatalogSpec = dict[str, SchemaSpec]  # catalog_name -> SchemaSpec


def assert_database_structure(result: DatabaseIntrospectionResult, expected_catalogs: CatalogSpec):
    def fail(msg: str, path: list[str]):
        full = ".".join(path)
        raise AssertionError(f"{msg} at {full}" if full else msg)

    def assert_keys(actual_dict: Mapping[str, Any], expected_dict: Mapping[str, Any], path: list[str], level: str):
        actual_keys = set(actual_dict)
        expected_keys = set(expected_dict)
        missing = expected_keys - actual_keys
        extra = actual_keys - expected_keys
        if missing or extra:
            fail(f"Unexpected {level}: missing={sorted(missing)}, extra={sorted(extra)}", path)

    def assert_columns(actual_columns: list[DatabaseColumn], expected_columns: list[DatabaseColumn], path: list[str]):
        actual_map = {c.name: c for c in actual_columns}
        expected_map = {c.name: c for c in expected_columns}
        assert_keys(actual_map, expected_map, path, "columns")
        for col_name, expected_column in expected_map.items():
            if actual_map[col_name] != expected_column:
                fail(f"Column {col_name} mismatch: expected={expected_column}, got={actual_map[col_name]}", path)

    actual_catalogs = {c.name: c for c in result.catalogs}
    assert_keys(actual_catalogs, expected_catalogs, [], "catalogs")

    for catalog_name, expected_schemas in expected_catalogs.items():
        actual_catalog = actual_catalogs[catalog_name]
        actual_schemas = {s.name: s for s in actual_catalog.schemas}

        assert_keys(actual_schemas, expected_schemas, [catalog_name], "schemas")

        for schema_name, expected_tables in expected_schemas.items():
            actual_schema = actual_schemas[schema_name]
            actual_tables = {t.name: t for t in actual_schema.tables}

            assert_keys(actual_tables, expected_tables, [catalog_name, schema_name], "tables")

            for table_name, expected_table_columns in expected_tables.items():
                table = actual_tables[table_name]
                assert_columns(table.columns, expected_table_columns, [catalog_name, schema_name, table_name])
