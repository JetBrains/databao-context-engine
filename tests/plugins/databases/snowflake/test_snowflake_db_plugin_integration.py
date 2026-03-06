"""Snowflake integration tests -- require real Snowflake credentials.

Set up:
  1. Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_DATABASE environment variables
  2. Set one of the following authentication methods:
     a) Password: SNOWFLAKE_PASSWORD
     b) Key pair: SNOWFLAKE_PRIVATE_KEY_FILE (and optionally SNOWFLAKE_PRIVATE_KEY_PWD)
     c) SSO: SNOWFLAKE_AUTHENTICATOR (e.g., "externalbrowser")
  3. Optionally set SNOWFLAKE_WAREHOUSE and SNOWFLAKE_ROLE
"""

from __future__ import annotations

import contextlib
import logging
import os
from typing import Any, Generator, Mapping, Sequence

import pytest
import snowflake.connector

from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.pluginlib.plugin_utils import (
    check_connection_for_datasource,
    execute_datasource_plugin,
)
from databao_context_engine.plugins.databases.databases_types import DatabaseIntrospectionResult
from databao_context_engine.plugins.databases.snowflake.snowflake_db_plugin import SnowflakeDbPlugin
from tests.plugins.databases.database_contracts import (
    ColumnIs,
    ColumnStatsExists,
    ForeignKeyExists,
    PrimaryKeyIs,
    SamplesCountIs,
    SamplesEqual,
    TableDescriptionContains,
    TableExists,
    TableKindIs,
    UniqueConstraintExists,
    assert_contract,
)

logger = logging.getLogger(__name__)

# Environment variables for test credentials
SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER = os.environ.get("SNOWFLAKE_USER", "")
SNOWFLAKE_DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE = os.environ.get("SNOWFLAKE_ROLE")

# Authentication options
SNOWFLAKE_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD")
SNOWFLAKE_PRIVATE_KEY_FILE = os.environ.get("SNOWFLAKE_PRIVATE_KEY_FILE")
SNOWFLAKE_PRIVATE_KEY_PWD = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PWD")
SNOWFLAKE_AUTHENTICATOR = os.environ.get("SNOWFLAKE_AUTHENTICATOR")

pytestmark = pytest.mark.skipif(
    not (SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER and SNOWFLAKE_DATABASE),
    reason=(
        "Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, and SNOWFLAKE_DATABASE env vars to run Snowflake integration tests. "
        "Also set one of: SNOWFLAKE_PASSWORD, SNOWFLAKE_PRIVATE_KEY_FILE, or SNOWFLAKE_AUTHENTICATOR for authentication."
    ),
)

_TABLE_PREFIX = "sf_test_"


def _create_config(
    account: str = SNOWFLAKE_ACCOUNT,
    user: str = SNOWFLAKE_USER,
    database: str = SNOWFLAKE_DATABASE,
    warehouse: str | None = SNOWFLAKE_WAREHOUSE,
    role: str | None = SNOWFLAKE_ROLE,
    datasource_name: str = "test_snowflake",
) -> Mapping[str, Any]:
    config: dict[str, Any] = {
        "type": "snowflake",
        "name": datasource_name,
        "connection": {
            "account": account,
            "user": user,
            "database": database,
        },
    }

    if warehouse:
        config["connection"]["warehouse"] = warehouse
    if role:
        config["connection"]["role"] = role

    if SNOWFLAKE_PASSWORD:
        config["connection"]["auth"] = {"password": SNOWFLAKE_PASSWORD}
    elif SNOWFLAKE_PRIVATE_KEY_FILE:
        auth: dict[str, Any] = {"private_key_file": SNOWFLAKE_PRIVATE_KEY_FILE}
        if SNOWFLAKE_PRIVATE_KEY_PWD:
            auth["private_key_file_pwd"] = SNOWFLAKE_PRIVATE_KEY_PWD
        config["connection"]["auth"] = auth
    elif SNOWFLAKE_AUTHENTICATOR:
        config["connection"]["auth"] = {"authenticator": SNOWFLAKE_AUTHENTICATOR}
    else:
        pytest.skip(
            "No authentication method configured (SNOWFLAKE_PASSWORD, SNOWFLAKE_PRIVATE_KEY_FILE, or SNOWFLAKE_AUTHENTICATOR)"
        )

    return config


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _snowflake_execute(conn: snowflake.connector.SnowflakeConnection, sql: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql)


def _fqn(table: str) -> str:
    return f'"{SNOWFLAKE_DATABASE}"."{_TABLE_PREFIX}schema"."{_TABLE_PREFIX}{table}"'


def _schema_ready(conn: snowflake.connector.SnowflakeConnection) -> bool:
    sql = f"""
        SELECT COUNT(*) AS cnt
        FROM "{SNOWFLAKE_DATABASE}".INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_SCHEMA = '{_TABLE_PREFIX}schema'
        AND TABLE_NAME = '{_TABLE_PREFIX}view_paid_orders'
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return row[0] > 0 if row else False
    except Exception:
        return False


def _init_demo_schema(conn: snowflake.connector.SnowflakeConnection) -> None:
    schema = f'"{SNOWFLAKE_DATABASE}"."{_TABLE_PREFIX}schema"'

    _snowflake_execute(conn, f"CREATE SCHEMA IF NOT EXISTS {schema}")

    if _schema_ready(conn):
        logger.info("Snowflake demo schema already set up -- skipping")
        return

    logger.info("Creating Snowflake demo schema in %s ...", schema)

    users = _fqn("users")
    products = _fqn("products")
    orders = _fqn("orders")
    order_items = _fqn("order_items")
    view = _fqn("view_paid_orders")

    _snowflake_execute(conn, f"DROP VIEW IF EXISTS {view}")
    _snowflake_execute(conn, f"DROP TABLE IF EXISTS {order_items}")
    _snowflake_execute(conn, f"DROP TABLE IF EXISTS {orders}")
    _snowflake_execute(conn, f"DROP TABLE IF EXISTS {products}")
    _snowflake_execute(conn, f"DROP TABLE IF EXISTS {users}")

    _snowflake_execute(
        conn,
        f"""
        CREATE TABLE {users} (
            user_id     INT IDENTITY(1,1),
            name        VARCHAR(255) NOT NULL,
            email       VARCHAR(255) NOT NULL,
            created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            active      BOOLEAN NOT NULL,

            CONSTRAINT uq_{_TABLE_PREFIX}users_email UNIQUE (email),

            PRIMARY KEY (user_id)
        ) COMMENT='Application users'
    """,
    )

    _snowflake_execute(
        conn,
        f"""
        CREATE TABLE {products} (
            product_id  INT IDENTITY(1,1),
            sku         VARCHAR(32) NOT NULL,
            price       DECIMAL(10,2) NOT NULL,
            description VARCHAR(1000),

            CONSTRAINT uq_{_TABLE_PREFIX}products_sku UNIQUE (sku),

            PRIMARY KEY (product_id)
        ) COMMENT='Products catalog'
    """,
    )

    _snowflake_execute(
        conn,
        f"""
        CREATE TABLE {orders} (
            order_id     INT IDENTITY(1,1),
            user_id      INT NOT NULL,
            order_number VARCHAR(64) NOT NULL,
            status       VARCHAR(16) NOT NULL DEFAULT 'PENDING',
            placed_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            amount_cents INT NOT NULL,

            CONSTRAINT uq_{_TABLE_PREFIX}orders_user_number UNIQUE (user_id, order_number),

            CONSTRAINT fk_{_TABLE_PREFIX}orders_user
                FOREIGN KEY (user_id) REFERENCES {users}(user_id),

            PRIMARY KEY (order_id)
        ) COMMENT='Customer orders'
    """,
    )

    _snowflake_execute(
        conn,
        f"""
        CREATE TABLE {order_items} (
            order_id         INT NOT NULL,
            product_id       INT NOT NULL,
            line_no          INT NOT NULL,
            quantity         INT NOT NULL,
            unit_price_cents INT NOT NULL,

            CONSTRAINT fk_{_TABLE_PREFIX}oi_order
                FOREIGN KEY (order_id) REFERENCES {orders}(order_id),

            CONSTRAINT fk_{_TABLE_PREFIX}oi_product
                FOREIGN KEY (product_id) REFERENCES {products}(product_id),

            PRIMARY KEY (order_id, product_id)
        ) COMMENT='Order line items'
    """,
    )

    _snowflake_execute(
        conn,
        f"""
        CREATE VIEW {view} AS
        SELECT order_id, user_id, placed_at, amount_cents
        FROM {orders}
        WHERE status = 'PAID'
    """,
    )

    logger.info("Snowflake demo schema created successfully")


@contextlib.contextmanager
def _seed_rows(
    conn: snowflake.connector.SnowflakeConnection,
    table: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    cleanup_tables: list[str] | None = None,
) -> Generator[None, None, None]:
    fqn = _fqn(table)
    cleanup_table_fqns = [_fqn(t) for t in (cleanup_tables or [table])]

    try:
        for t in cleanup_table_fqns:
            _snowflake_execute(conn, f"DELETE FROM {t}")

        if rows:
            columns = list(rows[0].keys())
            columns_sql = ", ".join(columns)
            placeholders = ", ".join(["?"] * len(columns))
            sql = f"INSERT INTO {fqn} ({columns_sql}) VALUES ({placeholders})"

            with conn.cursor() as cur:
                data = [tuple(r[c] for c in columns) for r in rows]
                cur.executemany(sql, data)

        yield
    finally:
        for t in cleanup_table_fqns:
            _snowflake_execute(conn, f"DELETE FROM {t}")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def snowflake_conn() -> Generator[snowflake.connector.SnowflakeConnection, None, None]:
    snowflake.connector.paramstyle = "qmark"

    config = _create_config()
    conn_props = config["connection"]
    kwargs = {k: v for k, v in conn_props.items() if k != "auth"}
    kwargs.update(conn_props.get("auth", {}))
    conn = snowflake.connector.connect(**kwargs)
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture(scope="module")
def sf_demo_schema(snowflake_conn: snowflake.connector.SnowflakeConnection) -> snowflake.connector.SnowflakeConnection:
    """Initialize demo schema - runs once per module."""
    _init_demo_schema(snowflake_conn)
    return snowflake_conn


# ---------------------------------------------------------------------------
# Integration Tests
# ---------------------------------------------------------------------------


def test_snowflake_check_connection():
    plugin = SnowflakeDbPlugin()
    config_file = _create_config()
    check_connection_for_datasource(plugin, DatasourceType(full_type="snowflake"), config_file)


def test_snowflake_plugin_execute(sf_demo_schema: snowflake.connector.SnowflakeConnection):
    plugin = SnowflakeDbPlugin()
    config_file = _create_config()
    result = execute_datasource_plugin(plugin, DatasourceType(full_type="snowflake"), config_file, "file_name")
    assert isinstance(result, DatabaseIntrospectionResult)

    assert_contract(
        result,
        [
            TableExists(SNOWFLAKE_DATABASE, f"{_TABLE_PREFIX}schema", f"{_TABLE_PREFIX}users"),
            TableExists(SNOWFLAKE_DATABASE, f"{_TABLE_PREFIX}schema", f"{_TABLE_PREFIX}products"),
            TableExists(SNOWFLAKE_DATABASE, f"{_TABLE_PREFIX}schema", f"{_TABLE_PREFIX}orders"),
            TableExists(SNOWFLAKE_DATABASE, f"{_TABLE_PREFIX}schema", f"{_TABLE_PREFIX}order_items"),
            TableExists(SNOWFLAKE_DATABASE, f"{_TABLE_PREFIX}schema", f"{_TABLE_PREFIX}view_paid_orders"),
        ],
    )


def test_snowflake_introspection_contract(sf_demo_schema: snowflake.connector.SnowflakeConnection):
    plugin = SnowflakeDbPlugin()
    config_file = _create_config()
    result = execute_datasource_plugin(plugin, DatasourceType(full_type="snowflake"), config_file, "file_name")
    assert isinstance(result, DatabaseIntrospectionResult)

    catalog = SNOWFLAKE_DATABASE
    schema = f"{_TABLE_PREFIX}schema"
    t_prefix = _TABLE_PREFIX

    assert_contract(
        result,
        [
            # -- users ---------------------------------------------------------
            TableExists(catalog, schema, f"{t_prefix}users"),
            TableKindIs(catalog, schema, f"{t_prefix}users", "table"),
            TableDescriptionContains(catalog, schema, f"{t_prefix}users", "Application users"),
            ColumnIs(
                catalog, schema, f"{t_prefix}users", "USER_ID", type="NUMBER", nullable=False, generated="identity"
            ),
            ColumnIs(catalog, schema, f"{t_prefix}users", "NAME", type="TEXT", nullable=False),
            ColumnIs(catalog, schema, f"{t_prefix}users", "EMAIL", type="TEXT", nullable=False),
            ColumnIs(catalog, schema, f"{t_prefix}users", "CREATED_AT", type="TIMESTAMP_NTZ", nullable=False),
            ColumnIs(catalog, schema, f"{t_prefix}users", "ACTIVE", type="BOOLEAN", nullable=False),
            PrimaryKeyIs(catalog, schema, f"{t_prefix}users", ["USER_ID"]),
            UniqueConstraintExists(
                catalog, schema, f"{t_prefix}users", ["EMAIL"], name=f"UQ_{t_prefix.upper()}USERS_EMAIL"
            ),
            # -- products ------------------------------------------------------
            TableExists(catalog, schema, f"{t_prefix}products"),
            TableKindIs(catalog, schema, f"{t_prefix}products", "table"),
            TableDescriptionContains(catalog, schema, f"{t_prefix}products", "Products catalog"),
            ColumnIs(
                catalog,
                schema,
                f"{t_prefix}products",
                "PRODUCT_ID",
                type="NUMBER",
                nullable=False,
                generated="identity",
            ),
            ColumnIs(catalog, schema, f"{t_prefix}products", "SKU", type="TEXT", nullable=False),
            ColumnIs(catalog, schema, f"{t_prefix}products", "PRICE", type="NUMBER", nullable=False),
            ColumnIs(catalog, schema, f"{t_prefix}products", "DESCRIPTION", type="TEXT", nullable=True),
            PrimaryKeyIs(catalog, schema, f"{t_prefix}products", ["PRODUCT_ID"]),
            UniqueConstraintExists(
                catalog, schema, f"{t_prefix}products", ["SKU"], name=f"UQ_{t_prefix.upper()}PRODUCTS_SKU"
            ),
            # -- orders --------------------------------------------------------
            TableExists(catalog, schema, f"{t_prefix}orders"),
            TableKindIs(catalog, schema, f"{t_prefix}orders", "table"),
            TableDescriptionContains(catalog, schema, f"{t_prefix}orders", "Customer orders"),
            ColumnIs(
                catalog, schema, f"{t_prefix}orders", "ORDER_ID", type="NUMBER", nullable=False, generated="identity"
            ),
            ColumnIs(catalog, schema, f"{t_prefix}orders", "USER_ID", type="NUMBER", nullable=False),
            ColumnIs(catalog, schema, f"{t_prefix}orders", "STATUS", type="TEXT", nullable=False),
            ColumnIs(catalog, schema, f"{t_prefix}orders", "AMOUNT_CENTS", type="NUMBER", nullable=False),
            PrimaryKeyIs(catalog, schema, f"{t_prefix}orders", ["ORDER_ID"]),
            UniqueConstraintExists(
                catalog,
                schema,
                f"{t_prefix}orders",
                ["USER_ID", "ORDER_NUMBER"],
                name=f"UQ_{t_prefix.upper()}ORDERS_USER_NUMBER",
            ),
            ForeignKeyExists(
                catalog,
                schema,
                f"{t_prefix}orders",
                from_columns=["USER_ID"],
                ref_table=f"{schema}.{t_prefix}users",
                ref_columns=["USER_ID"],
                name=f"FK_{t_prefix.upper()}ORDERS_USER",
            ),
            # -- order_items ---------------------------------------------------
            TableExists(catalog, schema, f"{t_prefix}order_items"),
            TableKindIs(catalog, schema, f"{t_prefix}order_items", "table"),
            TableDescriptionContains(catalog, schema, f"{t_prefix}order_items", "Order line items"),
            ColumnIs(catalog, schema, f"{t_prefix}order_items", "ORDER_ID", type="NUMBER", nullable=False),
            ColumnIs(catalog, schema, f"{t_prefix}order_items", "PRODUCT_ID", type="NUMBER", nullable=False),
            ColumnIs(catalog, schema, f"{t_prefix}order_items", "QUANTITY", type="NUMBER", nullable=False),
            PrimaryKeyIs(catalog, schema, f"{t_prefix}order_items", ["ORDER_ID", "PRODUCT_ID"]),
            ForeignKeyExists(
                catalog,
                schema,
                f"{t_prefix}order_items",
                from_columns=["ORDER_ID"],
                ref_table=f"{schema}.{t_prefix}orders",
                ref_columns=["ORDER_ID"],
                name=f"FK_{t_prefix.upper()}OI_ORDER",
            ),
            ForeignKeyExists(
                catalog,
                schema,
                f"{t_prefix}order_items",
                from_columns=["PRODUCT_ID"],
                ref_table=f"{schema}.{t_prefix}products",
                ref_columns=["PRODUCT_ID"],
                name=f"FK_{t_prefix.upper()}OI_PRODUCT",
            ),
            # -- view ----------------------------------------------------------
            TableExists(catalog, schema, f"{t_prefix}view_paid_orders"),
            TableKindIs(catalog, schema, f"{t_prefix}view_paid_orders", "view"),
        ],
    )


def test_snowflake_exact_samples(sf_demo_schema: snowflake.connector.SnowflakeConnection):
    users_rows = [
        {
            "user_id": 1,
            "name": "Alice",
            "email": "alice@test.com",
            "created_at": "2025-01-01T00:00:00",
            "active": True,
        },
        {
            "user_id": 2,
            "name": "Bob",
            "email": "bob@test.com",
            "created_at": "2025-01-02T00:00:00",
            "active": False,
        },
    ]

    products_rows = [
        {"product_id": 1, "sku": "SKU-1", "price": 10.50, "description": "Product A"},
        {"product_id": 2, "sku": "SKU-2", "price": 20.00, "description": None},
        {"product_id": 3, "sku": "SKU-3", "price": 15.75, "description": "Product C"},
    ]

    with _seed_rows(sf_demo_schema, "users", users_rows, cleanup_tables=["orders", "users"]):
        with _seed_rows(sf_demo_schema, "products", products_rows):
            plugin = SnowflakeDbPlugin()
            config_file = _create_config()
            result = execute_datasource_plugin(plugin, DatasourceType(full_type="snowflake"), config_file, "file_name")
            assert isinstance(result, DatabaseIntrospectionResult)

            catalog = SNOWFLAKE_DATABASE
            schema = f"{_TABLE_PREFIX}schema"
            t_prefix = _TABLE_PREFIX

            assert_contract(
                result,
                [
                    TableExists(catalog, schema, f"{t_prefix}users"),
                    SamplesEqual(catalog, schema, f"{t_prefix}users", users_rows),
                    TableExists(catalog, schema, f"{t_prefix}products"),
                    SamplesEqual(catalog, schema, f"{t_prefix}products", products_rows),
                    TableExists(catalog, schema, f"{t_prefix}orders"),
                    SamplesCountIs(catalog, schema, f"{t_prefix}orders", 0),
                    TableExists(catalog, schema, f"{t_prefix}order_items"),
                    SamplesCountIs(catalog, schema, f"{t_prefix}order_items", 0),
                ],
            )


def test_snowflake_table_and_column_statistics(sf_demo_schema: snowflake.connector.SnowflakeConnection):
    rows = [
        {"product_id": 1, "sku": "SKU-A", "price": 10.50, "description": "Product A"},
        {"product_id": 2, "sku": "SKU-B", "price": 10.50, "description": "Product B"},
        {"product_id": 3, "sku": "SKU-C", "price": 10.50, "description": "Product C"},
        {"product_id": 4, "sku": "SKU-D", "price": 30.00, "description": "Product D"},
        {"product_id": 5, "sku": "SKU-E", "price": 30.00, "description": "Product E"},
        {"product_id": 6, "sku": "SKU-F", "price": 20.00, "description": None},
        {"product_id": 7, "sku": "SKU-G", "price": 40.00, "description": None},
        {"product_id": 8, "sku": "SKU-H", "price": 50.00, "description": None},
    ]

    with _seed_rows(sf_demo_schema, "products", rows, cleanup_tables=["order_items", "products"]):
        plugin = SnowflakeDbPlugin()
        config_file = _create_config()
        result = execute_datasource_plugin(plugin, DatasourceType(full_type="snowflake"), config_file, "file_name")
        assert isinstance(result, DatabaseIntrospectionResult)

        assert_contract(
            result,
            [
                TableExists(SNOWFLAKE_DATABASE, f"{_TABLE_PREFIX}schema", f"{_TABLE_PREFIX}products"),
                ColumnStatsExists(
                    SNOWFLAKE_DATABASE,
                    f"{_TABLE_PREFIX}schema",
                    f"{_TABLE_PREFIX}products",
                    "PRICE",
                    null_count=0,
                    non_null_count=8,
                    distinct_count=5,
                    min_value="10.50",
                    max_value="50.00",
                    total_row_count=8,
                ),
                ColumnStatsExists(
                    SNOWFLAKE_DATABASE,
                    f"{_TABLE_PREFIX}schema",
                    f"{_TABLE_PREFIX}products",
                    "DESCRIPTION",
                    null_count=3,
                    non_null_count=5,
                    distinct_count=5,
                    min_value="Product A",
                    max_value="Product E",
                    total_row_count=8,
                ),
            ],
        )


def test_snowflake_high_cardinality_statistics(sf_demo_schema: snowflake.connector.SnowflakeConnection):
    rows = [
        {"product_id": i, "sku": f"SKU-{i:04d}", "price": float(i * 10), "description": f"Product {i}"}
        for i in range(1, 151)
    ]

    relative_error = 0.03  # average HLL error should be 1.63%
    with _seed_rows(sf_demo_schema, "products", rows, cleanup_tables=["order_items", "products"]):
        plugin = SnowflakeDbPlugin()
        config_file = _create_config()
        result = execute_datasource_plugin(plugin, DatasourceType(full_type="snowflake"), config_file, "file_name")
        assert isinstance(result, DatabaseIntrospectionResult)

        assert_contract(
            result,
            [
                TableExists(SNOWFLAKE_DATABASE, f"{_TABLE_PREFIX}schema", f"{_TABLE_PREFIX}products"),
                ColumnStatsExists(
                    SNOWFLAKE_DATABASE,
                    f"{_TABLE_PREFIX}schema",
                    f"{_TABLE_PREFIX}products",
                    "PRICE",
                    null_count=0,
                    non_null_count=150,
                    distinct_count=150,
                    distinct_count_tolerance=relative_error,
                    min_value="10.00",
                    max_value="1500.00",
                    total_row_count=150,
                ),
                ColumnStatsExists(
                    SNOWFLAKE_DATABASE,
                    f"{_TABLE_PREFIX}schema",
                    f"{_TABLE_PREFIX}products",
                    "DESCRIPTION",
                    distinct_count=150,
                    distinct_count_tolerance=relative_error,
                ),
            ],
        )
