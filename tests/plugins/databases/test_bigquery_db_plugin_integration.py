"""BigQuery integration tests -- require a real GCP project with ADC.

Set up:
  1. Set BIGQUERY_TEST_PROJECT to your GCP project ID
  2. Set BIGQUERY_TEST_DATASET to an existing dataset name in that project
  3. Authenticate via ADC: run 'gcloud auth application-default login' locally,
     or set GOOGLE_APPLICATION_CREDENTIALS to a service account key file in CI
"""

from __future__ import annotations

import contextlib
import logging
import os
from typing import Any, Generator, Mapping, Sequence

import pytest
from google.cloud import bigquery

from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.pluginlib.plugin_utils import (
    check_connection_for_datasource,
    execute_datasource_plugin,
)
from databao_context_engine.plugins.databases.bigquery.bigquery_db_plugin import BigQueryDbPlugin
from databao_context_engine.plugins.databases.databases_types import DatabaseIntrospectionResult
from tests.plugins.databases.database_contracts import (
    ColumnIs,
    ForeignKeyExists,
    PrimaryKeyIs,
    SamplesCountIs,
    TableDescriptionContains,
    TableExists,
    TableKindIs,
    assert_contract,
    log_introspection_result,
)

logger = logging.getLogger(__name__)

BQ_PROJECT = os.environ.get("BIGQUERY_TEST_PROJECT", "")
BQ_DATASET = os.environ.get("BIGQUERY_TEST_DATASET", "")

pytestmark = pytest.mark.skipif(
    not (BQ_PROJECT and BQ_DATASET),
    reason=(
        "Set BIGQUERY_TEST_PROJECT and BIGQUERY_TEST_DATASET env vars to run BigQuery integration tests. "
        "Authentication uses Application Default Credentials (ADC): run "
        "'gcloud auth application-default login' locally, or set GOOGLE_APPLICATION_CREDENTIALS "
        "to a service account key file path in CI."
    ),
)

_TABLE_PREFIX = "bq_test_"


def _create_config(
    project: str = BQ_PROJECT,
    dataset: str = BQ_DATASET,
    location: str | None = None,
    datasource_name: str = "test_bigquery",
) -> Mapping[str, Any]:
    config: dict[str, Any] = {
        "type": "bigquery",
        "name": datasource_name,
        "connection": {
            "project": project,
        },
        "introspection_scope": {
            "include": [{"catalog": project, "schemas": [dataset]}],
        },
    }
    if location:
        config["connection"]["location"] = location
    return config


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bq_execute(client: bigquery.Client, sql: str) -> None:
    client.query(sql).result()


def _fqn(table: str) -> str:
    """Fully-qualified table name with backtick quoting."""
    return f"`{BQ_PROJECT}`.`{BQ_DATASET}`.`{_TABLE_PREFIX}{table}`"


def _schema_ready(client: bigquery.Client) -> bool:
    """Check if the demo schema is fully set up (the view is the last thing created)."""
    sql = (
        f"SELECT COUNT(*) AS cnt "
        f"FROM `{BQ_PROJECT}`.`{BQ_DATASET}`.INFORMATION_SCHEMA.VIEWS "
        f"WHERE table_name = '{_TABLE_PREFIX}view_paid_orders'"
    )
    try:
        rows = list(client.query(sql).result())
        return rows[0]["cnt"] > 0
    except Exception:
        return False


def _init_demo_schema(client: bigquery.Client) -> None:
    """Idempotently create the dataset, demo tables with inline constraints, descriptions, and view.

    UNIQUE constraints can only be defined inline in CREATE TABLE (not via
    ALTER TABLE), so we drop and recreate if a previous partial run left
    tables without constraints.
    """
    _bq_execute(client, f"CREATE SCHEMA IF NOT EXISTS `{BQ_PROJECT}`.`{BQ_DATASET}`")

    if _schema_ready(client):
        logger.info("BigQuery demo schema already set up -- skipping")
        return

    logger.info("Creating BigQuery demo schema in %s.%s ...", BQ_PROJECT, BQ_DATASET)

    users = _fqn("users")
    products = _fqn("products")
    orders = _fqn("orders")
    order_items = _fqn("order_items")
    view = _fqn("view_paid_orders")

    # Drop in reverse dependency order to allow clean recreation
    _bq_execute(client, f"DROP VIEW IF EXISTS {view}")
    _bq_execute(client, f"DROP TABLE IF EXISTS {order_items}")
    _bq_execute(client, f"DROP TABLE IF EXISTS {orders}")
    _bq_execute(client, f"DROP TABLE IF EXISTS {products}")
    _bq_execute(client, f"DROP TABLE IF EXISTS {users}")

    # -- Tables with inline constraints (all NOT ENFORCED) ---------------------
    _bq_execute(
        client,
        f"""
        CREATE TABLE {users} (
            user_id     INT64 NOT NULL,
            name        STRING NOT NULL,
            email       STRING NOT NULL,
            created_at  TIMESTAMP NOT NULL,
            active      BOOL NOT NULL,

            PRIMARY KEY (user_id) NOT ENFORCED
        )
    """,
    )

    _bq_execute(
        client,
        f"""
        CREATE TABLE {products} (
            product_id  INT64 NOT NULL,
            sku         STRING NOT NULL,
            price       NUMERIC NOT NULL,
            description STRING,

            PRIMARY KEY (product_id) NOT ENFORCED
        )
    """,
    )

    _bq_execute(
        client,
        f"""
        CREATE TABLE {orders} (
            order_id     INT64 NOT NULL,
            user_id      INT64 NOT NULL,
            order_number STRING NOT NULL,
            status       STRING NOT NULL,
            placed_at    TIMESTAMP NOT NULL,
            amount_cents INT64 NOT NULL,

            PRIMARY KEY (order_id) NOT ENFORCED,
            FOREIGN KEY (user_id) REFERENCES {users}(user_id) NOT ENFORCED
        )
    """,
    )

    _bq_execute(
        client,
        f"""
        CREATE TABLE {order_items} (
            order_id         INT64 NOT NULL,
            product_id       INT64 NOT NULL,
            line_no          INT64 NOT NULL,
            quantity         INT64 NOT NULL,
            unit_price_cents INT64 NOT NULL,

            PRIMARY KEY (order_id, product_id) NOT ENFORCED,
            FOREIGN KEY (order_id) REFERENCES {orders}(order_id) NOT ENFORCED,
            FOREIGN KEY (product_id) REFERENCES {products}(product_id) NOT ENFORCED
        )
    """,
    )

    # -- Descriptions ----------------------------------------------------------
    _bq_execute(client, f"ALTER TABLE {users} SET OPTIONS (description = 'Application users')")
    _bq_execute(client, f"ALTER TABLE {users} ALTER COLUMN email SET OPTIONS (description = 'User email address')")

    # -- View ------------------------------------------------------------------
    _bq_execute(
        client,
        f"""
        CREATE OR REPLACE VIEW {view} AS
        SELECT order_id, user_id, placed_at, amount_cents
        FROM {orders}
        WHERE status = 'PAID'
    """,
    )

    logger.info("BigQuery demo schema created successfully")


@contextlib.contextmanager
def _seed_rows(
    client: bigquery.Client,
    table: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    cleanup_tables: list[str] | None = None,
) -> Generator[None, None, None]:
    """Insert sample rows and clean up afterwards."""
    fqn = _fqn(table)
    cleanup_fqns = [_fqn(t) for t in (cleanup_tables or [table])]

    for t in cleanup_fqns:
        _bq_execute(client, f"DELETE FROM {t} WHERE TRUE")

    try:
        if rows:
            columns = list(rows[0].keys())
            col_sql = ", ".join(columns)
            value_rows = []
            for r in rows:
                vals = []
                for c in columns:
                    v = r[c]
                    if v is None:
                        vals.append("NULL")
                    elif isinstance(v, bool):
                        vals.append("TRUE" if v else "FALSE")
                    elif isinstance(v, (int, float)):
                        vals.append(str(v))
                    elif isinstance(v, str):
                        vals.append(f"'{v}'")
                    else:
                        vals.append(f"'{v}'")
                value_rows.append(f"({', '.join(vals)})")
            values_sql = ", ".join(value_rows)
            _bq_execute(client, f"INSERT INTO {fqn} ({col_sql}) VALUES {values_sql}")
        yield
    finally:
        for t in cleanup_fqns:
            _bq_execute(client, f"DELETE FROM {t} WHERE TRUE")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def bq_demo_schema() -> Generator[bigquery.Client, None, None]:
    client = bigquery.Client(project=BQ_PROJECT)
    _init_demo_schema(client)
    yield client


# ---------------------------------------------------------------------------
# Moved integration tests (originally in test_bigquery_db_plugin.py)
# ---------------------------------------------------------------------------


def test_bigquery_plugin_execute(bq_demo_schema: bigquery.Client):
    plugin = BigQueryDbPlugin()
    config_file = _create_config()
    result = execute_datasource_plugin(plugin, DatasourceType(full_type="bigquery"), config_file, "file_name")
    assert isinstance(result, DatabaseIntrospectionResult)

    log_introspection_result(result)

    assert len(result.catalogs) == 1
    catalog = result.catalogs[0]
    assert catalog.name == BQ_PROJECT
    schema_names = [s.name for s in catalog.schemas]
    assert BQ_DATASET in schema_names


def test_bigquery_check_connection():
    plugin = BigQueryDbPlugin()
    config_file = _create_config()
    check_connection_for_datasource(plugin, DatasourceType(full_type="bigquery"), config_file, "file_name")


def test_bigquery_run_sql_with_params():
    from pydantic import TypeAdapter

    from databao_context_engine.plugins.databases.bigquery.config_file import BigQueryConfigFile

    plugin = BigQueryDbPlugin()
    config = TypeAdapter(BigQueryConfigFile).validate_python(_create_config())

    result = plugin.run_sql(
        file_config=config,
        sql="SELECT ? AS str_val, ? AS int_val, ? AS float_val, ? AS bool_val",
        params=["hello", 42, 3.14, True],
    )
    assert result.columns == ["str_val", "int_val", "float_val", "bool_val"]
    assert len(result.rows) == 1
    row = result.rows[0]
    assert row[0] == "hello"
    assert row[1] == 42
    assert row[2] == pytest.approx(3.14)
    assert row[3] is True


def test_bigquery_run_sql_with_array_param():
    from pydantic import TypeAdapter

    from databao_context_engine.plugins.databases.bigquery.config_file import BigQueryConfigFile

    plugin = BigQueryDbPlugin()
    config = TypeAdapter(BigQueryConfigFile).validate_python(_create_config())

    result = plugin.run_sql(
        file_config=config,
        sql="SELECT val FROM UNNEST(?) AS val ORDER BY val",
        params=[["alpha", "bravo", "charlie"]],
    )
    assert result.columns == ["val"]
    assert [r[0] for r in result.rows] == ["alpha", "bravo", "charlie"]


# ---------------------------------------------------------------------------
# Contract-based introspection tests
# ---------------------------------------------------------------------------


def test_bigquery_introspection_contract(bq_demo_schema: bigquery.Client):
    plugin = BigQueryDbPlugin()
    config_file = _create_config()
    result = execute_datasource_plugin(plugin, DatasourceType(full_type="bigquery"), config_file, "file_name")
    assert isinstance(result, DatabaseIntrospectionResult)

    log_introspection_result(result)

    cat = BQ_PROJECT
    sch = BQ_DATASET
    p = _TABLE_PREFIX

    assert_contract(
        result,
        [
            # -- users ---------------------------------------------------------
            TableExists(cat, sch, f"{p}users"),
            TableKindIs(cat, sch, f"{p}users", "table"),
            TableDescriptionContains(cat, sch, f"{p}users", "Application users"),
            ColumnIs(cat, sch, f"{p}users", "user_id", type="INT64", nullable=False),
            ColumnIs(cat, sch, f"{p}users", "name", type="STRING", nullable=False),
            ColumnIs(
                cat,
                sch,
                f"{p}users",
                "email",
                type="STRING",
                nullable=False,
                description_contains="User email address",
            ),
            ColumnIs(cat, sch, f"{p}users", "created_at", type="TIMESTAMP", nullable=False),
            ColumnIs(cat, sch, f"{p}users", "active", type="BOOL", nullable=False),
            PrimaryKeyIs(cat, sch, f"{p}users", ["user_id"]),
            # -- products ------------------------------------------------------
            TableExists(cat, sch, f"{p}products"),
            TableKindIs(cat, sch, f"{p}products", "table"),
            ColumnIs(cat, sch, f"{p}products", "product_id", type="INT64", nullable=False),
            ColumnIs(cat, sch, f"{p}products", "sku", type="STRING", nullable=False),
            ColumnIs(cat, sch, f"{p}products", "price", type="NUMERIC", nullable=False),
            ColumnIs(cat, sch, f"{p}products", "description", type="STRING", nullable=True),
            PrimaryKeyIs(cat, sch, f"{p}products", ["product_id"]),
            # -- orders --------------------------------------------------------
            TableExists(cat, sch, f"{p}orders"),
            TableKindIs(cat, sch, f"{p}orders", "table"),
            ColumnIs(cat, sch, f"{p}orders", "order_id", type="INT64", nullable=False),
            ColumnIs(cat, sch, f"{p}orders", "user_id", type="INT64", nullable=False),
            ColumnIs(cat, sch, f"{p}orders", "status", type="STRING", nullable=False),
            ColumnIs(cat, sch, f"{p}orders", "amount_cents", type="INT64", nullable=False),
            PrimaryKeyIs(cat, sch, f"{p}orders", ["order_id"]),
            ForeignKeyExists(
                cat,
                sch,
                f"{p}orders",
                from_columns=["user_id"],
                ref_table=f"{sch}.{p}users",
                ref_columns=["user_id"],
                enforced=False,
            ),
            # -- order_items ---------------------------------------------------
            TableExists(cat, sch, f"{p}order_items"),
            TableKindIs(cat, sch, f"{p}order_items", "table"),
            ColumnIs(cat, sch, f"{p}order_items", "order_id", type="INT64", nullable=False),
            ColumnIs(cat, sch, f"{p}order_items", "product_id", type="INT64", nullable=False),
            ColumnIs(cat, sch, f"{p}order_items", "quantity", type="INT64", nullable=False),
            ColumnIs(cat, sch, f"{p}order_items", "unit_price_cents", type="INT64", nullable=False),
            PrimaryKeyIs(cat, sch, f"{p}order_items", ["order_id", "product_id"]),
            ForeignKeyExists(
                cat,
                sch,
                f"{p}order_items",
                from_columns=["order_id"],
                ref_table=f"{sch}.{p}orders",
                ref_columns=["order_id"],
                enforced=False,
            ),
            ForeignKeyExists(
                cat,
                sch,
                f"{p}order_items",
                from_columns=["product_id"],
                ref_table=f"{sch}.{p}products",
                ref_columns=["product_id"],
                enforced=False,
            ),
            # -- view ----------------------------------------------------------
            TableExists(cat, sch, f"{p}view_paid_orders"),
            TableKindIs(cat, sch, f"{p}view_paid_orders", "view"),
        ],
    )


def test_bigquery_exact_samples(bq_demo_schema: bigquery.Client):
    """Seed rows and verify sample introspection. Requires billing (DML is not free-tier)."""
    rows = [
        {
            "user_id": 1,
            "name": "Alice",
            "email": "alice@test.com",
            "created_at": "2025-01-01 00:00:00 UTC",
            "active": True,
        },
        {
            "user_id": 2,
            "name": "Bob",
            "email": "bob@test.com",
            "created_at": "2025-01-02 00:00:00 UTC",
            "active": False,
        },
    ]

    try:
        with _seed_rows(bq_demo_schema, "users", rows, cleanup_tables=["order_items", "orders", "users"]):
            plugin = BigQueryDbPlugin()
            config_file = _create_config()
            result = execute_datasource_plugin(plugin, DatasourceType(full_type="bigquery"), config_file, "file_name")
            assert isinstance(result, DatabaseIntrospectionResult)

            assert_contract(
                result,
                [
                    TableExists(BQ_PROJECT, BQ_DATASET, f"{_TABLE_PREFIX}users"),
                    SamplesCountIs(BQ_PROJECT, BQ_DATASET, f"{_TABLE_PREFIX}users", 2),
                ],
            )
    except Exception as exc:
        if "billingNotEnabled" in str(exc) or "Billing has not been enabled" in str(exc):
            pytest.skip("DML requires billing to be enabled on the BigQuery project")
        raise
