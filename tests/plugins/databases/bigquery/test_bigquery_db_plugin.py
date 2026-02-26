import pytest

from databao_context_engine.plugins.databases.bigquery.bigquery_db_plugin import BigQueryDbPlugin


def test_bigquery_plugin_basics():
    plugin = BigQueryDbPlugin()
    assert plugin.id == "jetbrains/bigquery"
    assert plugin.supported == {"bigquery"}
    assert "bigquery" in plugin.supported_types()


def test_bigquery_config_validation():
    from pydantic import TypeAdapter

    from databao_context_engine.plugins.databases.bigquery.config_file import BigQueryConfigFile

    adapter = TypeAdapter(BigQueryConfigFile)

    config = adapter.validate_python(
        {
            "name": "my-bq",
            "type": "bigquery",
            "connection": {"project": "my-project"},
        }
    )
    assert config.connection.project == "my-project"
    assert config.connection.dataset is None
    assert config.connection.location is None


def test_bigquery_config_with_dataset():
    from pydantic import TypeAdapter

    from databao_context_engine.plugins.databases.bigquery.config_file import BigQueryConfigFile

    adapter = TypeAdapter(BigQueryConfigFile)

    config = adapter.validate_python(
        {
            "name": "my-bq",
            "type": "bigquery",
            "connection": {"project": "my-project", "dataset": "my_dataset"},
        }
    )
    assert config.connection.project == "my-project"
    assert config.connection.dataset == "my_dataset"


def test_bigquery_config_with_service_account_key_file():
    from pydantic import TypeAdapter

    from databao_context_engine.plugins.databases.bigquery.config_file import BigQueryConfigFile

    adapter = TypeAdapter(BigQueryConfigFile)

    config = adapter.validate_python(
        {
            "name": "my-bq",
            "type": "bigquery",
            "connection": {
                "project": "my-project",
                "location": "US",
                "auth": {"credentials_file": "/path/to/key.json"},
            },
        }
    )
    assert config.connection.project == "my-project"
    assert config.connection.location == "US"


def test_bigquery_config_with_service_account_json():
    from pydantic import TypeAdapter

    from databao_context_engine.plugins.databases.bigquery.config_file import BigQueryConfigFile

    adapter = TypeAdapter(BigQueryConfigFile)

    config = adapter.validate_python(
        {
            "name": "my-bq",
            "type": "bigquery",
            "connection": {
                "project": "my-project",
                "auth": {"credentials_json": '{"type": "service_account"}'},
            },
        }
    )
    assert config.connection.project == "my-project"


def test_bigquery_quote_ident_normal():
    from databao_context_engine.plugins.databases.bigquery.bigquery_introspector import BigQueryIntrospector

    assert BigQueryIntrospector._quote_ident("my_table") == "`my_table`"


def test_bigquery_quote_ident_with_backtick():
    from databao_context_engine.plugins.databases.bigquery.bigquery_introspector import BigQueryIntrospector

    assert BigQueryIntrospector._quote_ident("my`table") == "`my``table`"


def test_bigquery_component_queries_include_uq_and_fks():
    from databao_context_engine.plugins.databases.bigquery.bigquery_introspector import BigQueryIntrospector

    introspector = BigQueryIntrospector()
    queries = introspector._component_queries("my-project", ["ds1"])
    assert "uq" in queries
    assert "fks" in queries
    assert "UNIQUE" in queries["uq"]
    assert "FOREIGN KEY" in queries["fks"]


def test_bigquery_sql_unique_constraints_structure():
    from databao_context_engine.plugins.databases.bigquery.bigquery_introspector import BigQueryIntrospector

    introspector = BigQueryIntrospector()
    sql = introspector._sql_unique_constraints("proj", ["ds1", "ds2"])

    assert "UNION ALL" in sql
    assert sql.count("constraint_type = 'UNIQUE'") == 2
    for alias in ["schema_name", "table_name", "constraint_name", "column_name", "position"]:
        assert alias in sql


def test_bigquery_sql_foreign_keys_structure():
    from databao_context_engine.plugins.databases.bigquery.bigquery_introspector import BigQueryIntrospector

    introspector = BigQueryIntrospector()
    sql = introspector._sql_foreign_keys("proj", ["ds1"])

    assert "FOREIGN KEY" in sql
    assert "CONSTRAINT_COLUMN_USAGE" in sql
    assert "position_in_unique_constraint" in sql
    for alias in [
        "schema_name",
        "table_name",
        "constraint_name",
        "position",
        "from_column",
        "ref_schema",
        "ref_table",
        "to_column",
        "enforced",
    ]:
        assert alias in sql


def test_bigquery_sql_foreign_keys_multi_schema():
    from databao_context_engine.plugins.databases.bigquery.bigquery_introspector import BigQueryIntrospector

    introspector = BigQueryIntrospector()
    sql = introspector._sql_foreign_keys("proj", ["ds1", "ds2", "ds3"])

    assert sql.count("UNION ALL") == 2
    assert sql.count("FOREIGN KEY") == 3


def test_bigquery_unique_constraints_model_builder():
    """Verify the model builder correctly assembles unique constraints from BigQuery-shaped rows."""
    from databao_context_engine.plugins.databases.introspection_model_builder import IntrospectionModelBuilder

    schemas = IntrospectionModelBuilder.build_schemas_from_components(
        schemas=["ds1"],
        rels=[{"schema_name": "ds1", "table_name": "users", "kind": "table"}],
        cols=[
            {
                "schema_name": "ds1",
                "table_name": "users",
                "column_name": "id",
                "ordinal_position": 1,
                "data_type": "INT64",
                "is_nullable": "NO",
            },
            {
                "schema_name": "ds1",
                "table_name": "users",
                "column_name": "email",
                "ordinal_position": 2,
                "data_type": "STRING",
                "is_nullable": "NO",
            },
        ],
        pk_cols=[
            {
                "schema_name": "ds1",
                "table_name": "users",
                "constraint_name": "pk_users",
                "column_name": "id",
                "position": 1,
            },
        ],
        uq_cols=[
            {
                "schema_name": "ds1",
                "table_name": "users",
                "constraint_name": "uq_email",
                "column_name": "email",
                "position": 1,
            },
        ],
    )

    assert len(schemas) == 1
    table = schemas[0].tables[0]
    assert table.primary_key is not None
    assert table.primary_key.columns == ["id"]
    assert len(table.unique_constraints) == 1
    assert table.unique_constraints[0].name == "uq_email"
    assert table.unique_constraints[0].columns == ["email"]


def test_bigquery_foreign_keys_model_builder():
    """Verify the model builder correctly assembles foreign keys from BigQuery-shaped rows."""
    from databao_context_engine.plugins.databases.introspection_model_builder import IntrospectionModelBuilder

    schemas = IntrospectionModelBuilder.build_schemas_from_components(
        schemas=["ds1"],
        rels=[
            {"schema_name": "ds1", "table_name": "orders", "kind": "table"},
            {"schema_name": "ds1", "table_name": "users", "kind": "table"},
        ],
        cols=[
            {
                "schema_name": "ds1",
                "table_name": "orders",
                "column_name": "id",
                "ordinal_position": 1,
                "data_type": "INT64",
                "is_nullable": "NO",
            },
            {
                "schema_name": "ds1",
                "table_name": "orders",
                "column_name": "user_id",
                "ordinal_position": 2,
                "data_type": "INT64",
                "is_nullable": "NO",
            },
            {
                "schema_name": "ds1",
                "table_name": "users",
                "column_name": "id",
                "ordinal_position": 1,
                "data_type": "INT64",
                "is_nullable": "NO",
            },
        ],
        fk_cols=[
            {
                "schema_name": "ds1",
                "table_name": "orders",
                "constraint_name": "fk_orders_user",
                "position": 1,
                "from_column": "user_id",
                "ref_schema": "ds1",
                "ref_table": "users",
                "to_column": "id",
                "enforced": "NO",
            },
        ],
    )

    assert len(schemas) == 1
    orders = next(t for t in schemas[0].tables if t.name == "orders")
    assert len(orders.foreign_keys) == 1
    fk = orders.foreign_keys[0]
    assert fk.name == "fk_orders_user"
    assert fk.referenced_table == "ds1.users"
    assert len(fk.mapping) == 1
    assert fk.mapping[0].from_column == "user_id"
    assert fk.mapping[0].to_column == "id"
    assert fk.enforced is False


def test_bigquery_composite_foreign_key_model_builder():
    """Verify composite foreign keys are correctly ordered and assembled."""
    from databao_context_engine.plugins.databases.introspection_model_builder import IntrospectionModelBuilder

    schemas = IntrospectionModelBuilder.build_schemas_from_components(
        schemas=["ds1"],
        rels=[
            {"schema_name": "ds1", "table_name": "line_items", "kind": "table"},
        ],
        cols=[
            {
                "schema_name": "ds1",
                "table_name": "line_items",
                "column_name": "order_id",
                "ordinal_position": 1,
                "data_type": "INT64",
                "is_nullable": "NO",
            },
            {
                "schema_name": "ds1",
                "table_name": "line_items",
                "column_name": "product_id",
                "ordinal_position": 2,
                "data_type": "INT64",
                "is_nullable": "NO",
            },
        ],
        fk_cols=[
            {
                "schema_name": "ds1",
                "table_name": "line_items",
                "constraint_name": "fk_composite",
                "position": 1,
                "from_column": "order_id",
                "ref_schema": "ds1",
                "ref_table": "order_products",
                "to_column": "order_id",
                "enforced": "YES",
            },
            {
                "schema_name": "ds1",
                "table_name": "line_items",
                "constraint_name": "fk_composite",
                "position": 2,
                "from_column": "product_id",
                "ref_schema": "ds1",
                "ref_table": "order_products",
                "to_column": "product_id",
                "enforced": "YES",
            },
        ],
    )

    assert len(schemas) == 1
    table = schemas[0].tables[0]
    assert len(table.foreign_keys) == 1
    fk = table.foreign_keys[0]
    assert fk.name == "fk_composite"
    assert fk.referenced_table == "ds1.order_products"
    assert len(fk.mapping) == 2
    assert fk.mapping[0].from_column == "order_id"
    assert fk.mapping[0].to_column == "order_id"
    assert fk.mapping[1].from_column == "product_id"
    assert fk.mapping[1].to_column == "product_id"
    assert fk.enforced is True


def test_bigquery_list_schemas_with_dataset_configured():
    from unittest.mock import MagicMock

    from google.cloud import bigquery as bq

    from databao_context_engine.plugins.databases.bigquery.bigquery_introspector import BigQueryIntrospector

    introspector = BigQueryIntrospector()
    connection = MagicMock()
    connection.default_query_job_config = bq.QueryJobConfig(
        default_dataset=bq.DatasetReference("my-project", "my_dataset"),
    )

    schemas = introspector._list_schemas_for_catalog(connection, "my-project")
    assert schemas == ["my_dataset"]
    connection.list_datasets.assert_not_called()


def test_bigquery_list_schemas_discovers_all_datasets():
    from unittest.mock import MagicMock

    from databao_context_engine.plugins.databases.bigquery.bigquery_introspector import BigQueryIntrospector

    introspector = BigQueryIntrospector()
    mock_ds1 = MagicMock()
    mock_ds1.dataset_id = "dataset_a"
    mock_ds2 = MagicMock()
    mock_ds2.dataset_id = "dataset_b"
    connection = MagicMock()
    connection.default_query_job_config = None
    connection.list_datasets.return_value = [mock_ds1, mock_ds2]

    schemas = introspector._list_schemas_for_catalog(connection, "my-project")
    assert schemas == ["dataset_a", "dataset_b"]
    connection.list_datasets.assert_called_once()


def test_bigquery_to_query_param_scalar_types():
    from google.cloud import bigquery as bq

    from databao_context_engine.plugins.databases.bigquery.bigquery_introspector import BigQueryIntrospector

    param = BigQueryIntrospector._to_query_param("hello")
    assert isinstance(param, bq.ScalarQueryParameter)
    assert param.type_ == "STRING"

    param = BigQueryIntrospector._to_query_param(42)
    assert isinstance(param, bq.ScalarQueryParameter)
    assert param.type_ == "INT64"

    param = BigQueryIntrospector._to_query_param(3.14)
    assert isinstance(param, bq.ScalarQueryParameter)
    assert param.type_ == "FLOAT64"

    param = BigQueryIntrospector._to_query_param(True)
    assert isinstance(param, bq.ScalarQueryParameter)
    assert param.type_ == "BOOL"


def test_bigquery_to_query_param_string_array():
    from google.cloud import bigquery as bq

    from databao_context_engine.plugins.databases.bigquery.bigquery_introspector import BigQueryIntrospector

    param = BigQueryIntrospector._to_query_param(["a", "b", "c"])
    assert isinstance(param, bq.ArrayQueryParameter)
    assert param.array_type == "STRING"


def test_bigquery_to_query_param_int_array():
    from google.cloud import bigquery as bq

    from databao_context_engine.plugins.databases.bigquery.bigquery_introspector import BigQueryIntrospector

    param = BigQueryIntrospector._to_query_param([1, 2, 3])
    assert isinstance(param, bq.ArrayQueryParameter)
    assert param.array_type == "INT64"
    assert param.values == [1, 2, 3]


def test_bigquery_to_query_param_float_array():
    from google.cloud import bigquery as bq

    from databao_context_engine.plugins.databases.bigquery.bigquery_introspector import BigQueryIntrospector

    param = BigQueryIntrospector._to_query_param([1.1, 2.2])
    assert isinstance(param, bq.ArrayQueryParameter)
    assert param.array_type == "FLOAT64"
    assert param.values == [1.1, 2.2]


def test_bigquery_to_query_param_bool_array():
    from google.cloud import bigquery as bq

    from databao_context_engine.plugins.databases.bigquery.bigquery_introspector import BigQueryIntrospector

    param = BigQueryIntrospector._to_query_param([True, False])
    assert isinstance(param, bq.ArrayQueryParameter)
    assert param.array_type == "BOOL"
    assert param.values == [True, False]


def test_bigquery_to_query_param_array_skips_leading_nones():
    from google.cloud import bigquery as bq

    from databao_context_engine.plugins.databases.bigquery.bigquery_introspector import BigQueryIntrospector

    param = BigQueryIntrospector._to_query_param([None, None, 42])
    assert isinstance(param, bq.ArrayQueryParameter)
    assert param.array_type == "INT64"


def test_bigquery_to_query_param_tuple_treated_as_array():
    from google.cloud import bigquery as bq

    from databao_context_engine.plugins.databases.bigquery.bigquery_introspector import BigQueryIntrospector

    param = BigQueryIntrospector._to_query_param(("x", "y"))
    assert isinstance(param, bq.ArrayQueryParameter)
    assert param.array_type == "STRING"


def test_bigquery_to_query_param_empty_array_defaults_to_string():
    from google.cloud import bigquery as bq

    from databao_context_engine.plugins.databases.bigquery.bigquery_introspector import BigQueryIntrospector

    param = BigQueryIntrospector._to_query_param([])
    assert isinstance(param, bq.ArrayQueryParameter)
    assert param.array_type == "STRING"


def test_bigquery_credentials_invalid_file():
    from databao_context_engine.plugins.databases.bigquery.bigquery_introspector import BigQueryIntrospector
    from databao_context_engine.plugins.databases.bigquery.config_file import (
        BigQueryConnectionProperties,
        BigQueryServiceAccountKeyFileAuth,
    )

    conn = BigQueryConnectionProperties(
        project="p",
        auth=BigQueryServiceAccountKeyFileAuth(credentials_file="/nonexistent/key.json"),
    )
    with pytest.raises(FileNotFoundError, match="credentials file not found"):
        BigQueryIntrospector._build_credentials(conn)


def test_bigquery_credentials_invalid_json():
    from databao_context_engine.plugins.databases.bigquery.bigquery_introspector import BigQueryIntrospector
    from databao_context_engine.plugins.databases.bigquery.config_file import (
        BigQueryConnectionProperties,
        BigQueryServiceAccountJsonAuth,
    )

    conn = BigQueryConnectionProperties(project="p", auth=BigQueryServiceAccountJsonAuth(credentials_json="not-json"))
    with pytest.raises(ValueError, match="not valid JSON"):
        BigQueryIntrospector._build_credentials(conn)
