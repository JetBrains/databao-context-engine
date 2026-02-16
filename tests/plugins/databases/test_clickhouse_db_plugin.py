import contextlib
from datetime import date
from typing import Any, Mapping, Sequence

import clickhouse_connect
import pytest
from testcontainers.clickhouse import ClickHouseContainer  # type: ignore

from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.pluginlib.plugin_utils import execute_datasource_plugin
from databao_context_engine.plugins.databases.clickhouse.clickhouse_db_plugin import ClickhouseDbPlugin
from databao_context_engine.plugins.databases.databases_types import (
    DatabaseIntrospectionResult,
)
from tests.plugins.databases.database_contracts import (
    ColumnIs,
    IndexExists,
    SamplesCountIs,
    SamplesEqual,
    TableDescriptionContains,
    TableExists,
    TableKindIs,
    assert_contract,
)

HTTP_PORT = 8123


@pytest.fixture(scope="module")
def clickhouse_container():
    container = ClickHouseContainer()
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="module")
def clickhouse_container_with_demo_schema(clickhouse_container: ClickHouseContainer):
    client = clickhouse_connect.get_client(
        host=clickhouse_container.get_container_host_ip(),
        port=int(clickhouse_container.get_exposed_port(HTTP_PORT)),
        username=clickhouse_container.username,
        password=clickhouse_container.password,
        database=clickhouse_container.dbname,
    )
    try:
        client.command("CREATE DATABASE IF NOT EXISTS custom")
        client.command("CREATE DATABASE IF NOT EXISTS ext")

        client.command(
            """
            CREATE TABLE custom.orders
            (
                order_id     UInt64 COMMENT 'Surrogate key',
                order_number String,
                status       LowCardinality(String) DEFAULT 'PENDING',
                placed_at    DateTime DEFAULT now(),
                amount_cents Int32,
                notes        Nullable(String) COMMENT 'Optional note',
                active       UInt8 DEFAULT 1,

                INDEX idx_order_lower       lowerUTF8(order_number) TYPE set(100) GRANULARITY 4,
                INDEX idx_placed_at_minmax  placed_at               TYPE minmax   GRANULARITY 1,
                INDEX idx_status_set        status                  TYPE set(3)   GRANULARITY 4
            )
            ENGINE = MergeTree
            ORDER BY order_id
            COMMENT 'Customer orders header'
            """
        )

        client.command(
            """
            CREATE TABLE custom.order_items
            (
                order_id           UInt64,
                line_no            UInt32,
                quantity           Int32,
                unit_price_cents   Int32,
                total_amount_cents Int32 MATERIALIZED quantity * unit_price_cents
            )
            ENGINE = MergeTree
            ORDER BY (order_id, line_no)
            COMMENT 'Line items per order'
            """
        )

        client.command(
            """
            CREATE VIEW custom.recent_paid_orders AS
            SELECT
                order_id,
                order_number,
                placed_at,
                amount_cents
            FROM custom.orders
            WHERE status = 'PAID'
            """
        )

        client.command(
            """
            CREATE TABLE custom.revenue_by_day
            (
                day Date,
                total_amount_cents Int64
            )
            ENGINE = SummingMergeTree
            ORDER BY day
            COMMENT 'Aggregated revenue per day'
            """
        )

        client.command(
            """
            CREATE MATERIALIZED VIEW custom.revenue_by_day_mv
            TO custom.revenue_by_day
            AS
            SELECT
                toDate(placed_at) AS day,
                sum(amount_cents) AS total_amount_cents
            FROM custom.orders
            GROUP BY day
            """
        )

        client.command(
            """
            CREATE TABLE ext.customers_file
            (
                customer_id  UInt64 COMMENT 'Surrogate key',
                email        String COMMENT 'Customer email address',
                full_name    String,
                country_code FixedString(2),
                created_at   DateTime
            )
            ENGINE = File('CSV', 'customers.csv')
            COMMENT 'External file-backed table (CSV in user_files)'
            """
        )
    finally:
        client.close()

    return clickhouse_container


@pytest.fixture
def create_clickhouse_client(clickhouse_container: ClickHouseContainer):
    def _create_client(database: str | None = None):
        return clickhouse_connect.get_client(
            host=clickhouse_container.get_container_host_ip(),
            port=int(clickhouse_container.get_exposed_port(HTTP_PORT)),
            username=clickhouse_container.username,
            password=clickhouse_container.password,
            database=database or clickhouse_container.dbname,
        )

    return _create_client


@contextlib.contextmanager
def seed_rows(
    create_clickhouse_client,
    full_table_name: str,
    rows: Sequence[Mapping[str, Any]],
):
    client = create_clickhouse_client()
    try:
        client.command(f"TRUNCATE TABLE {full_table_name}")

        if rows:
            columns = list(rows[0].keys())
            data = [tuple(r[c] for c in columns) for r in rows]
            client.insert(full_table_name, data, column_names=columns)

        yield
    finally:
        client.command(f"TRUNCATE TABLE {full_table_name}")
        client.close()


def test_clickhouse_plugin_execute(clickhouse_container_with_demo_schema: ClickHouseContainer):
    plugin = ClickhouseDbPlugin()
    config_file = _create_config_file_from_container(clickhouse_container_with_demo_schema)
    result = execute_datasource_plugin(plugin, DatasourceType(full_type=config_file["type"]), config_file, "file_name")
    assert isinstance(result, DatabaseIntrospectionResult)

    assert_contract(
        result,
        [
            TableExists("clickhouse", "custom", "orders"),
            TableKindIs("clickhouse", "custom", "orders", "table"),
            TableDescriptionContains("clickhouse", "custom", "orders", "Customer orders header"),
            ColumnIs(
                "clickhouse",
                "custom",
                "orders",
                "order_id",
                type="UInt64",
                nullable=False,
                description_contains="Surrogate key",
            ),
            ColumnIs("clickhouse", "custom", "orders", "order_number", type="String", nullable=False),
            ColumnIs(
                "clickhouse",
                "custom",
                "orders",
                "status",
                type="LowCardinality(String)",
                nullable=False,
                default_contains="PENDING",
            ),
            ColumnIs(
                "clickhouse", "custom", "orders", "placed_at", type="DateTime", nullable=False, default_contains="now()"
            ),
            ColumnIs("clickhouse", "custom", "orders", "amount_cents", type="Int32", nullable=False),
            ColumnIs(
                "clickhouse",
                "custom",
                "orders",
                "notes",
                type="Nullable(String)",
                nullable=True,
                description_contains="Optional note",
            ),
            ColumnIs("clickhouse", "custom", "orders", "active", type="UInt8", nullable=False, default_equals="1"),
            IndexExists(
                "clickhouse",
                "custom",
                "orders",
                name="idx_order_lower",
                columns=["lowerUTF8(order_number)"],
                method="set",
            ),
            IndexExists(
                "clickhouse",
                "custom",
                "orders",
                name="idx_placed_at_minmax",
                columns=["placed_at"],
                method="minmax",
            ),
            IndexExists(
                "clickhouse",
                "custom",
                "orders",
                name="idx_status_set",
                columns=["status"],
                method="set",
            ),
            TableExists("clickhouse", "custom", "order_items"),
            TableKindIs("clickhouse", "custom", "order_items", "table"),
            TableDescriptionContains("clickhouse", "custom", "order_items", "Line items per order"),
            ColumnIs(
                "clickhouse",
                "custom",
                "order_items",
                "total_amount_cents",
                type="Int32",
                nullable=False,
                generated="computed",
                default_contains="unit_price_cents",
            ),
            TableExists("clickhouse", "custom", "recent_paid_orders"),
            TableKindIs("clickhouse", "custom", "recent_paid_orders", "view"),
            ColumnIs("clickhouse", "custom", "recent_paid_orders", "order_id", type="UInt64", nullable=False),
            ColumnIs("clickhouse", "custom", "recent_paid_orders", "order_number", type="String", nullable=False),
            ColumnIs("clickhouse", "custom", "recent_paid_orders", "placed_at", type="DateTime", nullable=False),
            ColumnIs("clickhouse", "custom", "recent_paid_orders", "amount_cents", type="Int32", nullable=False),
            TableExists("clickhouse", "custom", "revenue_by_day"),
            TableKindIs("clickhouse", "custom", "revenue_by_day", "table"),
            TableDescriptionContains("clickhouse", "custom", "revenue_by_day", "Aggregated revenue per day"),
            ColumnIs("clickhouse", "custom", "revenue_by_day", "day", type="Date", nullable=False),
            ColumnIs("clickhouse", "custom", "revenue_by_day", "total_amount_cents", type="Int64", nullable=False),
            TableExists("clickhouse", "custom", "revenue_by_day_mv"),
            TableKindIs("clickhouse", "custom", "revenue_by_day_mv", "materialized_view"),
            TableExists("clickhouse", "ext", "customers_file"),
            TableKindIs("clickhouse", "ext", "customers_file", "external_table"),
            TableDescriptionContains("clickhouse", "ext", "customers_file", "External file-backed table"),
            ColumnIs(
                "clickhouse",
                "ext",
                "customers_file",
                "customer_id",
                type="UInt64",
                nullable=False,
                description_contains="Surrogate key",
            ),
            ColumnIs(
                "clickhouse",
                "ext",
                "customers_file",
                "email",
                type="String",
                nullable=False,
                description_contains="Customer email address",
            ),
            ColumnIs("clickhouse", "ext", "customers_file", "country_code", type="FixedString(2)", nullable=False),
            ColumnIs("clickhouse", "ext", "customers_file", "created_at", type="DateTime", nullable=False),
        ],
    )


def test_clickhouse_exact_samples(
    create_clickhouse_client,
    clickhouse_container_with_demo_schema: ClickHouseContainer,
):
    rows = [
        {"day": date(2026, 1, 1), "total_amount_cents": 123},
        {"day": date(2026, 1, 2), "total_amount_cents": 456},
    ]

    with seed_rows(create_clickhouse_client, "custom.revenue_by_day", rows):
        plugin = ClickhouseDbPlugin()
        config_file = _create_config_file_from_container(clickhouse_container_with_demo_schema)
        result = execute_datasource_plugin(
            plugin, DatasourceType(full_type=config_file["type"]), config_file, "file_name"
        )
        assert isinstance(result, DatabaseIntrospectionResult)

        assert_contract(
            result,
            [
                SamplesEqual("clickhouse", "custom", "revenue_by_day", rows=rows),
            ],
        )


def test_clickhouse_samples_in_big(
    create_clickhouse_client,
    clickhouse_container_with_demo_schema: ClickHouseContainer,
):
    plugin = ClickhouseDbPlugin()
    limit = plugin._introspector._SAMPLE_LIMIT

    rows = [{"day": date(2026, 1, (i % 28) + 1), "total_amount_cents": i} for i in range(1000)]

    with seed_rows(create_clickhouse_client, "custom.revenue_by_day", rows):
        config_file = _create_config_file_from_container(clickhouse_container_with_demo_schema)
        result = execute_datasource_plugin(
            plugin, DatasourceType(full_type=config_file["type"]), config_file, "file_name"
        )
        assert isinstance(result, DatabaseIntrospectionResult)

        assert_contract(
            result,
            [
                SamplesCountIs("clickhouse", "custom", "revenue_by_day", count=limit),
            ],
        )


def _create_config_file_from_container(
    clickhouse: ClickHouseContainer, datasource_name: str | None = "file_name"
) -> Mapping[str, Any]:
    return {
        "type": "databases/clickhouse",
        "name": datasource_name,
        "connection": {
            "host": clickhouse.get_container_host_ip(),
            "port": int(clickhouse.get_exposed_port(HTTP_PORT)),
            # TODO now this parameter is not used in introspections, worth checking if that is expected behavior
            "database": clickhouse.dbname,
            "username": clickhouse.username,
            "password": clickhouse.password,
        },
    }
