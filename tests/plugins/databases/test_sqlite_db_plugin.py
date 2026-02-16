import contextlib
import sqlite3
from pathlib import Path
from typing import Any, Mapping, Sequence

import pytest

from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.pluginlib.plugin_utils import execute_datasource_plugin
from databao_context_engine.plugins.databases.databases_types import DatabaseIntrospectionResult
from databao_context_engine.plugins.databases.sqlite.sqlite_db_plugin import SQLiteDbPlugin
from tests.plugins.databases.database_contracts import (
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
def temp_sqlite_file(tmp_path: Path):
    db_file = tmp_path / "test_db.sqlite"
    yield db_file


def execute_sqlite_queries(db_file: Path, *queries: str):
    conn = sqlite3.connect(database=str(db_file))
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        with conn:
            for q in queries:
                conn.execute(q)
    finally:
        conn.close()


@contextlib.contextmanager
def seed_rows_sqlite(db_file: Path, table: str, rows: Sequence[Mapping[str, Any]]):
    conn = sqlite3.connect(database=str(db_file))
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(f"DELETE FROM {table}")

        if rows:
            columns = list(rows[0].keys())
            placeholders = ", ".join(["?"] * len(columns))
            col_sql = ", ".join(columns)
            sql = f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})"
            data = [tuple(r[c] for c in columns) for r in rows]
            conn.executemany(sql, data)

        conn.commit()
        yield
    finally:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute(f"DELETE FROM {table}")
        conn.commit()
        conn.close()


@pytest.fixture
def sqlite_with_demo_schema(temp_sqlite_file: Path):
    execute_sqlite_queries(
        temp_sqlite_file,
        """
        CREATE TABLE users (
            user_id   INTEGER NOT NULL,
            name      VARCHAR NOT NULL,
            email     VARCHAR NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,

            CONSTRAINT pk_users PRIMARY KEY (user_id),
            CONSTRAINT uq_users_email UNIQUE (email),
            CONSTRAINT chk_users_email CHECK (email LIKE '%@%')
        );
        """.strip(),
        "CREATE INDEX idx_users_name ON users(name);",
        """
        CREATE TABLE products (
            product_id  INTEGER NOT NULL,
            sku         VARCHAR NOT NULL,
            price       DECIMAL(10,2) NOT NULL,

            CONSTRAINT pk_products PRIMARY KEY (product_id),
            CONSTRAINT uq_products_sku UNIQUE (sku),
            CONSTRAINT chk_products_price CHECK (price >= 0)
        );
        """.strip(),
        """
        CREATE TABLE orders (
            order_id     INTEGER NOT NULL,
            user_id      INTEGER NOT NULL,
            order_number VARCHAR NOT NULL,
            status       VARCHAR NOT NULL DEFAULT 'PENDING',
            placed_at    TIMESTAMP NOT NULL,
            amount_cents INTEGER NOT NULL,

            CONSTRAINT pk_orders PRIMARY KEY (order_id),
            CONSTRAINT uq_orders_user_number UNIQUE (user_id, order_number),

            CONSTRAINT chk_orders_status CHECK (status IN ('PENDING', 'PAID', 'CANCELLED')),
            CONSTRAINT chk_orders_amount CHECK (amount_cents >= 0),

            CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(user_id)
        );
        """.strip(),
        "CREATE INDEX idx_orders_user_placed_at ON orders(user_id, placed_at);",
        """
        CREATE TABLE order_items (
            order_id         INTEGER NOT NULL,
            product_id       INTEGER NOT NULL,
            line_no          INTEGER NOT NULL,
            quantity         INTEGER NOT NULL,
            unit_price_cents INTEGER NOT NULL,

            CONSTRAINT pk_order_items PRIMARY KEY (order_id, product_id),

            CONSTRAINT fk_oi_order   FOREIGN KEY (order_id)   REFERENCES orders(order_id),
            CONSTRAINT fk_oi_product FOREIGN KEY (product_id) REFERENCES products(product_id),

            CONSTRAINT chk_oi_quantity   CHECK (quantity > 0),
            CONSTRAINT chk_oi_unit_price CHECK (unit_price_cents >= 0)
        );
        """.strip(),
        "CREATE INDEX idx_oi_product ON order_items(product_id);",
        """
        CREATE VIEW recent_paid_orders AS
        SELECT
            order_id,
            user_id,
            placed_at,
            amount_cents
        FROM orders
        WHERE status = 'PAID';
        """.strip(),
    )
    return temp_sqlite_file


def _create_config_file_from_sqlite(sqlite_path: Path, datasource_name: str | None = "file_name") -> Mapping[str, Any]:
    return {
        "type": "databases/sqlite",
        "name": datasource_name,
        "connection": dict(database_path=str(sqlite_path)),
    }


def test_sqlite_plugin_introspection_demo_schema(sqlite_with_demo_schema: Path):
    plugin = SQLiteDbPlugin()
    config = _create_config_file_from_sqlite(sqlite_with_demo_schema)
    result = execute_datasource_plugin(plugin, DatasourceType(full_type=config["type"]), config, "file_name")
    assert isinstance(result, DatabaseIntrospectionResult)

    assert_contract(
        result,
        [
            TableExists("default", "main", "users"),
            TableKindIs("default", "main", "users", "table"),
            ColumnIs("default", "main", "users", "user_id", type="INTEGER", nullable=False),
            ColumnIs("default", "main", "users", "name", type="VARCHAR", nullable=False),
            ColumnIs("default", "main", "users", "email", type="VARCHAR", nullable=False),
            ColumnIs("default", "main", "users", "is_active", type="INTEGER", nullable=False, default_equals="1"),
            PrimaryKeyIs("default", "main", "users", ["user_id"]),
            UniqueConstraintExists("default", "main", "users", ["email"]),
            TableExists("default", "main", "products"),
            TableKindIs("default", "main", "products", "table"),
            ColumnIs("default", "main", "products", "product_id", type="INTEGER", nullable=False),
            ColumnIs("default", "main", "products", "sku", type="VARCHAR", nullable=False),
            ColumnIs("default", "main", "products", "price", type="DECIMAL(10,2)", nullable=False),
            PrimaryKeyIs("default", "main", "products", ["product_id"]),
            UniqueConstraintExists("default", "main", "products", ["sku"]),
            TableExists("default", "main", "orders"),
            TableKindIs("default", "main", "orders", "table"),
            ColumnIs("default", "main", "orders", "order_id", type="INTEGER", nullable=False),
            ColumnIs("default", "main", "orders", "user_id", type="INTEGER", nullable=False),
            ColumnIs("default", "main", "orders", "order_number", type="VARCHAR", nullable=False),
            ColumnIs(
                "default",
                "main",
                "orders",
                "status",
                type="VARCHAR",
                nullable=False,
                default_contains="PENDING",
            ),
            ColumnIs("default", "main", "orders", "placed_at", type="TIMESTAMP", nullable=False),
            ColumnIs("default", "main", "orders", "amount_cents", type="INTEGER", nullable=False),
            PrimaryKeyIs("default", "main", "orders", ["order_id"]),
            UniqueConstraintExists("default", "main", "orders", ["user_id", "order_number"]),
            ForeignKeyExists(
                "default",
                "main",
                "orders",
                from_columns=["user_id"],
                ref_table="main.users",
                ref_columns=["user_id"],
            ),
            IndexExists(
                "default", "main", "orders", name="idx_orders_user_placed_at", columns=["user_id", "placed_at"]
            ),
            TableExists("default", "main", "order_items"),
            TableKindIs("default", "main", "order_items", "table"),
            PrimaryKeyIs("default", "main", "order_items", ["order_id", "product_id"]),
            ForeignKeyExists(
                "default",
                "main",
                "order_items",
                from_columns=["order_id"],
                ref_table="main.orders",
                ref_columns=["order_id"],
            ),
            ForeignKeyExists(
                "default",
                "main",
                "order_items",
                from_columns=["product_id"],
                ref_table="main.products",
                ref_columns=["product_id"],
            ),
            IndexExists("default", "main", "order_items", name="idx_oi_product", columns=["product_id"]),
            TableExists("default", "main", "recent_paid_orders"),
            TableKindIs("default", "main", "recent_paid_orders", "view"),
            ColumnIs("default", "main", "recent_paid_orders", "order_id"),
            ColumnIs("default", "main", "recent_paid_orders", "user_id"),
            ColumnIs("default", "main", "recent_paid_orders", "placed_at"),
            ColumnIs("default", "main", "recent_paid_orders", "amount_cents"),
        ],
    )


def test_sqlite_exact_samples(sqlite_with_demo_schema: Path):
    rows = [
        {"user_id": 1, "name": "Andrew", "email": "andrew@example.com", "is_active": 1},
        {"user_id": 2, "name": "Boris", "email": "boris@example.com", "is_active": 1},
        {"user_id": 3, "name": "Cathy", "email": "cathy@example.com", "is_active": 1},
    ]

    with seed_rows_sqlite(sqlite_with_demo_schema, "users", rows):
        plugin = SQLiteDbPlugin()
        config = _create_config_file_from_sqlite(sqlite_with_demo_schema)
        result = execute_datasource_plugin(plugin, DatasourceType(full_type=config["type"]), config, "file_name")
        assert isinstance(result, DatabaseIntrospectionResult)
        assert_contract(result, [SamplesEqual("default", "main", "users", rows=rows)])


def test_sqlite_samples_in_big(sqlite_with_demo_schema: Path):
    plugin = SQLiteDbPlugin()
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

    with seed_rows_sqlite(sqlite_with_demo_schema, "users", rows):
        config = _create_config_file_from_sqlite(sqlite_with_demo_schema)
        result = execute_datasource_plugin(plugin, DatasourceType(full_type=config["type"]), config, "file_name")
        assert isinstance(result, DatabaseIntrospectionResult)

        assert_contract(result, [SamplesCountIs("default", "main", "users", count=limit)])
