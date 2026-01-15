import contextlib
from pathlib import Path
from typing import Any, Mapping, Sequence

import duckdb
import pytest

from nemory.pluginlib.build_plugin import DatasourceType
from nemory.pluginlib.plugin_utils import execute_datasource_plugin
from nemory.plugins.databases.databases_types import (
    DatabaseIntrospectionResult,
)
from nemory.plugins.duckdb_db_plugin import DuckDbPlugin
from tests.plugins.database_contracts import (
    CheckConstraintExists,
    ColumnIs,
    ForeignKeyExists,
    IndexExists,
    PrimaryKeyIs,
    SamplesCountIs,
    SamplesEqual,
    TableExists,
    TableKindIs,
    UniqueConstraintExists,
    assert_contract,
)


@pytest.fixture
def temp_duckdb_file(tmp_path: Path):
    db_file = tmp_path / "test_db.duckdb"
    yield db_file


def execute_duckdb_queries(db_file: Path, *queries: str):
    conn = duckdb.connect(database=str(db_file))
    with conn:
        for query in queries:
            conn.execute(query)


@contextlib.contextmanager
def seed_rows(db_file: Path, full_table_name: str, rows: Sequence[Mapping[str, Any]]):
    conn = duckdb.connect(database=str(db_file))
    try:
        conn.execute(f"TRUNCATE TABLE {full_table_name}")

        if rows:
            columns = list(rows[0].keys())

            placeholders = ", ".join(["?"] * len(columns))
            col_sql = ", ".join(columns)
            sql = f"INSERT INTO {full_table_name} ({col_sql}) VALUES ({placeholders})"
            data = [tuple(r[c] for c in columns) for r in rows]
            conn.executemany(sql, data)

        yield
    finally:
        conn.execute(f"TRUNCATE TABLE {full_table_name}")
        conn.close()


@pytest.fixture
def duckdb_with_demo_schema(temp_duckdb_file: Path):
    execute_duckdb_queries(
        temp_duckdb_file,
        "CREATE SCHEMA custom",
        "CREATE TYPE custom.order_status AS ENUM ('PENDING', 'PAID', 'CANCELLED')",
        """
        CREATE TABLE custom.users (
            user_id   INTEGER NOT NULL,
            name      VARCHAR NOT NULL,
            email     VARCHAR NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,

            CONSTRAINT pk_users PRIMARY KEY (user_id),
            CONSTRAINT uq_users_email UNIQUE (email),
            CONSTRAINT chk_users_email CHECK (email LIKE '%@%')
        )
        """,
        "CREATE INDEX idx_users_name ON custom.users(name)",
        """
        CREATE TABLE custom.products (
            product_id  INTEGER NOT NULL,
            sku         VARCHAR NOT NULL,
            price       DECIMAL(10,2) NOT NULL,

            CONSTRAINT pk_products PRIMARY KEY (product_id),
            CONSTRAINT uq_products_sku UNIQUE (sku),
            CONSTRAINT chk_products_price CHECK (price >= 0)
        )
        """,
        """
        CREATE TABLE custom.orders (
            order_id     INTEGER NOT NULL,
            user_id      INTEGER NOT NULL,
            order_number VARCHAR NOT NULL,
            status       custom.order_status NOT NULL DEFAULT 'PENDING',
            placed_at    TIMESTAMP NOT NULL,
            amount_cents INTEGER NOT NULL,

            CONSTRAINT pk_orders PRIMARY KEY (order_id),
            CONSTRAINT uq_orders_user_number UNIQUE (user_id, order_number),

            CONSTRAINT chk_orders_status CHECK (status IN ('PENDING', 'PAID', 'CANCELLED')),
            CONSTRAINT chk_orders_amount CHECK (amount_cents >= 0),

            CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES custom.users(user_id)
        )
        """,
        "CREATE INDEX idx_orders_user_placed_at ON custom.orders(user_id, placed_at)",
        """
        CREATE TABLE custom.order_items (
            order_id         INTEGER NOT NULL,
            product_id       INTEGER NOT NULL,
            line_no          INTEGER NOT NULL,
            quantity         INTEGER NOT NULL,
            unit_price_cents INTEGER NOT NULL,

            CONSTRAINT pk_order_items PRIMARY KEY (order_id, product_id),

            CONSTRAINT fk_oi_order   FOREIGN KEY (order_id)   REFERENCES custom.orders(order_id),
            CONSTRAINT fk_oi_product FOREIGN KEY (product_id) REFERENCES custom.products(product_id),

            CONSTRAINT chk_oi_quantity   CHECK (quantity > 0),
            CONSTRAINT chk_oi_unit_price CHECK (unit_price_cents >= 0)
        )
        """,
        "CREATE INDEX idx_oi_product ON custom.order_items(product_id)",
        """
        CREATE VIEW custom.recent_paid_orders AS
        SELECT
            order_id,
            user_id,
            placed_at,
            amount_cents
        FROM custom.orders
        WHERE status = 'PAID'
        """,
    )
    return temp_duckdb_file


def test_duckdb_plugin_introspection_demo_schema(duckdb_with_demo_schema: Path):
    plugin = DuckDbPlugin()
    config = _create_config_file_from_container(duckdb_with_demo_schema)
    result = execute_datasource_plugin(plugin, DatasourceType(full_type=config["type"]), config, "file_name")
    assert isinstance(result, DatabaseIntrospectionResult)

    assert_contract(
        result,
        [
            TableExists("test_db", "custom", "users"),
            TableKindIs("test_db", "custom", "users", "table"),
            ColumnIs("test_db", "custom", "users", "user_id", type="INTEGER", nullable=False),
            ColumnIs("test_db", "custom", "users", "name", type="VARCHAR", nullable=False),
            ColumnIs("test_db", "custom", "users", "email", type="VARCHAR", nullable=False),
            ColumnIs("test_db", "custom", "users", "is_active", type="INTEGER", nullable=False, default_equals="1"),
            PrimaryKeyIs("test_db", "custom", "users", ["user_id"]),
            UniqueConstraintExists("test_db", "custom", "users", ["email"]),
            CheckConstraintExists("test_db", "custom", "users", expression_contains="@"),
            TableExists("test_db", "custom", "products"),
            TableKindIs("test_db", "custom", "products", "table"),
            ColumnIs("test_db", "custom", "products", "product_id", type="INTEGER", nullable=False),
            ColumnIs("test_db", "custom", "products", "sku", type="VARCHAR", nullable=False),
            ColumnIs("test_db", "custom", "products", "price", type="DECIMAL(10,2)", nullable=False),
            PrimaryKeyIs("test_db", "custom", "products", ["product_id"]),
            UniqueConstraintExists("test_db", "custom", "products", ["sku"]),
            CheckConstraintExists("test_db", "custom", "products", expression_contains="price"),
            TableExists("test_db", "custom", "orders"),
            TableKindIs("test_db", "custom", "orders", "table"),
            ColumnIs("test_db", "custom", "orders", "order_id", type="INTEGER", nullable=False),
            ColumnIs("test_db", "custom", "orders", "user_id", type="INTEGER", nullable=False),
            ColumnIs("test_db", "custom", "orders", "order_number", type="VARCHAR", nullable=False),
            ColumnIs(
                "test_db",
                "custom",
                "orders",
                "status",
                type="ENUM('PENDING', 'PAID', 'CANCELLED')",
                nullable=False,
                default_contains="PENDING",
            ),
            ColumnIs("test_db", "custom", "orders", "placed_at", type="TIMESTAMP", nullable=False),
            ColumnIs("test_db", "custom", "orders", "amount_cents", type="INTEGER", nullable=False),
            PrimaryKeyIs("test_db", "custom", "orders", ["order_id"]),
            UniqueConstraintExists("test_db", "custom", "orders", ["user_id", "order_number"]),
            ForeignKeyExists(
                "test_db",
                "custom",
                "orders",
                from_columns=["user_id"],
                ref_table="custom.users",
                ref_columns=["user_id"],
            ),
            CheckConstraintExists("test_db", "custom", "orders", expression_contains="amount_cents"),
            IndexExists(
                "test_db", "custom", "orders", name="idx_orders_user_placed_at", columns=["user_id", "placed_at"]
            ),
            TableExists("test_db", "custom", "order_items"),
            TableKindIs("test_db", "custom", "order_items", "table"),
            PrimaryKeyIs("test_db", "custom", "order_items", ["order_id", "product_id"]),
            ForeignKeyExists(
                "test_db",
                "custom",
                "order_items",
                from_columns=["order_id"],
                ref_table="custom.orders",
                ref_columns=["order_id"],
            ),
            ForeignKeyExists(
                "test_db",
                "custom",
                "order_items",
                from_columns=["product_id"],
                ref_table="custom.products",
                ref_columns=["product_id"],
            ),
            CheckConstraintExists("test_db", "custom", "order_items", expression_contains="quantity"),
            CheckConstraintExists("test_db", "custom", "order_items", expression_contains="unit_price_cents"),
            IndexExists("test_db", "custom", "order_items", name="idx_oi_product", columns=["product_id"]),
            TableExists("test_db", "custom", "recent_paid_orders"),
            TableKindIs("test_db", "custom", "recent_paid_orders", "view"),
            ColumnIs("test_db", "custom", "recent_paid_orders", "order_id", type="INTEGER"),
            ColumnIs("test_db", "custom", "recent_paid_orders", "user_id", type="INTEGER"),
            ColumnIs("test_db", "custom", "recent_paid_orders", "placed_at", type="TIMESTAMP"),
            ColumnIs("test_db", "custom", "recent_paid_orders", "amount_cents", type="INTEGER"),
        ],
    )


def test_duckdb_exact_samples(duckdb_with_demo_schema: Path):
    rows = [
        {"user_id": 1, "name": "Andrew", "email": "andrew@example.com", "is_active": 1},
        {"user_id": 2, "name": "Boris", "email": "boris@example.com", "is_active": 1},
        {"user_id": 3, "name": "Cathy", "email": "cathy@example.com", "is_active": 1},
    ]

    with seed_rows(duckdb_with_demo_schema, "custom.users", rows):
        plugin = DuckDbPlugin()
        config = _create_config_file_from_container(duckdb_with_demo_schema)
        result = execute_datasource_plugin(plugin, DatasourceType(full_type=config["type"]), config, "file_name")
        assert isinstance(result, DatabaseIntrospectionResult)
        assert_contract(
            result,
            [
                SamplesEqual("test_db", "custom", "users", rows=rows),
            ],
        )


def test_duckdb_samples_in_big(duckdb_with_demo_schema: Path):
    plugin = DuckDbPlugin()
    limit = plugin._introspector._SAMPLE_LIMIT
    rows = [
        {
            "user_id": i,
            "name": f"name{i}",
            "email": f"user{i}@example.com",
            "is_active": 1,
        }
        for i in range(1, 1000)
    ]
    with seed_rows(duckdb_with_demo_schema, "custom.users", rows):
        config = _create_config_file_from_container(duckdb_with_demo_schema)
        result = execute_datasource_plugin(plugin, DatasourceType(full_type=config["type"]), config, "file_name")
        assert isinstance(result, DatabaseIntrospectionResult)

        assert_contract(
            result,
            [
                SamplesCountIs("test_db", "custom", "users", count=limit),
            ],
        )


def _create_config_file_from_container(duckdb_path: Path) -> Mapping[str, Any]:
    return {
        "type": "databases/duckdb",
        "connection": dict(database=str(duckdb_path)),
    }
