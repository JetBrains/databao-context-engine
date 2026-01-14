import asyncio
import contextlib
from datetime import datetime
from typing import Any, Mapping, Sequence

import asyncpg
import pytest
from pytest_unordered import unordered
from testcontainers.postgres import PostgresContainer  # type: ignore

from nemory.pluginlib.build_plugin import BuildExecutionResult, DatasourceType, EmbeddableChunk
from nemory.pluginlib.plugin_utils import execute_datasource_plugin
from nemory.plugins.databases.database_chunker import DatabaseColumnChunkContent, DatabaseTableChunkContent
from nemory.plugins.databases.databases_types import (
    DatabaseCatalog,
    DatabaseColumn,
    DatabaseIntrospectionResult,
    DatabasePartitionInfo,
    DatabaseSchema,
    DatabaseTable,
)
from nemory.plugins.postgresql_db_plugin import PostgresqlDbPlugin
from tests.plugins.database_contracts import (
    CheckConstraintExists,
    ColumnIs,
    ForeignKeyExists,
    IndexExists,
    PartitionMetaContains,
    PrimaryKeyIs,
    SamplesCountIs,
    SamplesEqual,
    TableDescriptionContains,
    TableExists,
    TableKindIs,
    UniqueConstraintExists,
    assert_contract,
)


@pytest.fixture(scope="module")
def postgres_container():
    container = PostgresContainer("postgres:18.0", driver=None)
    container.start()
    yield container
    container.stop()


def _get_connect_kwargs(postgres_container: PostgresContainer) -> dict[str, Any]:
    return {
        "host": postgres_container.get_container_host_ip(),
        "port": int(postgres_container.get_exposed_port(postgres_container.port)),
        "database": postgres_container.dbname,
        "user": postgres_container.username,
        "password": postgres_container.password,
    }


def _execute(postgres_container: PostgresContainer, sql: str) -> None:
    async def _run():
        conn = await asyncpg.connect(**_get_connect_kwargs(postgres_container))
        try:
            await conn.execute(sql)
        finally:
            await conn.close()

    asyncio.run(_run())


def _executemany(postgres_container: PostgresContainer, sql: str, args) -> None:
    async def _run():
        conn = await asyncpg.connect(**_get_connect_kwargs(postgres_container))
        try:
            await conn.executemany(sql, args)
        finally:
            await conn.close()

    asyncio.run(_run())


@pytest.fixture
def create_db_schema(postgres_container: PostgresContainer, request):
    @contextlib.contextmanager
    def _create_db_schema(desired_schema_name: str | None = None):
        schema_name = desired_schema_name or request.function.__name__
        _execute(postgres_container, f"CREATE SCHEMA {schema_name};")
        try:
            yield schema_name
        finally:
            _execute(postgres_container, f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")

    return _create_db_schema


@contextlib.contextmanager
def seed_rows(
    postgres_container: PostgresContainer,
    schema_name: str,
    table_name: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    cleanup_tables: list[str] | None = None,
):
    cleanup_tables = cleanup_tables or [table_name]

    for t in cleanup_tables:
        _execute(postgres_container, f"DELETE FROM {schema_name}.{t};")

    try:
        if rows:
            columns = list(rows[0].keys())

            col_sql = ", ".join(columns)
            placeholders = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
            sql = f"INSERT INTO {schema_name}.{table_name} ({col_sql}) VALUES ({placeholders});"
            args = [tuple(r[c] for c in columns) for r in rows]

            _executemany(postgres_container, sql, args)

        yield
    finally:
        for t in cleanup_tables:
            _execute(postgres_container, f"DELETE FROM {schema_name}.{t};")


def test_postgres_exact_samples(create_db_schema, postgres_container: PostgresContainer):
    schema_name = "custom"
    with create_db_schema(schema_name):
        _init_with_demo_schema(postgres_container, schema_name)

        rows = [
            {"product_id": 1, "sku": "SKU-1", "price": 10.50, "description": "foo"},
            {"product_id": 2, "sku": "SKU-2", "price": 20.00, "description": None},
        ]

        cleanup = ["order_items", "products"]

        with seed_rows(
            postgres_container,
            schema_name,
            "products",
            rows,
            cleanup_tables=cleanup,
        ):
            plugin = PostgresqlDbPlugin()
            config_file = _create_config_file_from_container(postgres_container)
            execution_result = execute_datasource_plugin(
                plugin, DatasourceType(full_type=config_file["type"]), config_file, "file_name"
            )
            assert isinstance(execution_result.result, DatabaseIntrospectionResult)

            assert_contract(
                execution_result.result,
                [
                    TableExists("test", schema_name, "products"),
                    SamplesEqual("test", schema_name, "products", rows=rows),
                ],
            )


def test_postgres_samples_in_big(create_db_schema, postgres_container: PostgresContainer):
    schema_name = "custom"
    with create_db_schema(schema_name):
        _init_with_demo_schema(postgres_container, schema_name)

        plugin = PostgresqlDbPlugin()
        limit = plugin._introspector._SAMPLE_LIMIT

        rows = [{"product_id": i, "sku": f"SKU-{i}", "price": float(i), "description": None} for i in range(1, 1000)]

        cleanup = ["order_items", "products"]

        with seed_rows(
            postgres_container,
            schema_name,
            "products",
            rows,
            cleanup_tables=cleanup,
        ):
            config_file = _create_config_file_from_container(postgres_container)
            execution_result = execute_datasource_plugin(
                plugin, DatasourceType(full_type=config_file["type"]), config_file, "file_name"
            )
            assert isinstance(execution_result.result, DatabaseIntrospectionResult)

            assert_contract(
                execution_result.result,
                [
                    TableExists("test", schema_name, "products"),
                    SamplesCountIs("test", schema_name, "products", count=limit),
                ],
            )


def test_postgres_partitions(create_db_schema, postgres_container):
    with create_db_schema() as db_schema:
        _execute(
            postgres_container,
            f"""
            CREATE TABLE {db_schema}.test_partitions (id int not null, name varchar(255) null)
            PARTITION BY RANGE (id);

            CREATE TABLE {db_schema}.test_partitions_1 PARTITION OF {db_schema}.test_partitions
            FOR VALUES FROM (0) TO (10);

            CREATE TABLE {db_schema}.test_partitions_2 PARTITION OF {db_schema}.test_partitions
            FOR VALUES FROM (10) TO (20)
            """,
        )

        plugin = PostgresqlDbPlugin()

        config_file = _create_config_file_from_container(postgres_container)

        execution_result = execute_datasource_plugin(
            plugin, DatasourceType(full_type=config_file["type"]), config_file, "file_name"
        )

        assert execution_result.result == DatabaseIntrospectionResult(
            [
                DatabaseCatalog(
                    "test",
                    [
                        DatabaseSchema(
                            db_schema,
                            tables=[
                                DatabaseTable(
                                    "test_partitions",
                                    [
                                        DatabaseColumn("id", "integer", False),
                                        DatabaseColumn("name", "character varying(255)", True),
                                    ],
                                    [],
                                    partition_info=DatabasePartitionInfo(
                                        meta={
                                            "columns_in_partition_key": ["id"],
                                            "partitioning_strategy": "range partitioned",
                                        },
                                        partition_tables=[
                                            "test_partitions_2", # In a subsequent PR, this will be fixed when we order keys and values in the introspections
                                            "test_partitions_1",
                                        ],
                                    ),
                                )
                            ],
                        ),
                    ],
                )
            ]
        )


def test_postgres_plugin_divide_into_chunks():
    plugin = PostgresqlDbPlugin()

    input = BuildExecutionResult(
        name="name",
        type="databases/postgres",
        description=None,
        version=None,
        executed_at=datetime.now(),
        result=DatabaseIntrospectionResult(
            catalogs=[
                DatabaseCatalog(
                    name="test",
                    schemas=[
                        DatabaseSchema(
                            name="public",
                            tables=[],
                        ),
                        DatabaseSchema(
                            name="custom",
                            tables=[
                                DatabaseTable(
                                    name="test",
                                    description="best table",
                                    columns=[
                                        DatabaseColumn(name="id", type="int4", nullable=False),
                                        DatabaseColumn(name="name", type="varchar", nullable=True),
                                    ],
                                    samples=[],
                                )
                            ],
                        ),
                    ],
                )
            ]
        ),
    )

    chunks = plugin.divide_result_into_chunks(input)

    assert len(chunks) == 3
    assert chunks == unordered(
        EmbeddableChunk(
            embeddable_text="Table test with columns id,name",
            content=DatabaseTableChunkContent(
                catalog_name="test",
                schema_name="custom",
                table=DatabaseTable(
                    name="test",
                    description="best table",
                    columns=[
                        DatabaseColumn(name="id", type="int4", nullable=False),
                        DatabaseColumn(name="name", type="varchar", nullable=True),
                    ],
                    samples=[],
                ),
            ),
        ),
        EmbeddableChunk(
            embeddable_text="Column id in table test",
            content=DatabaseColumnChunkContent(
                catalog_name="test",
                schema_name="custom",
                table_name="test",
                column=DatabaseColumn(name="id", type="int4", nullable=False),
            ),
        ),
        EmbeddableChunk(
            embeddable_text="Column name in table test",
            content=DatabaseColumnChunkContent(
                catalog_name="test",
                schema_name="custom",
                table_name="test",
                column=DatabaseColumn(name="name", type="varchar", nullable=True),
            ),
        ),
    )


def _init_with_demo_schema(postgres_container, schema_name: str):
    _execute(
        postgres_container,
        f"""
            CREATE TABLE {schema_name}.users (
                user_id     integer GENERATED BY DEFAULT AS IDENTITY,
                name        text NOT NULL,
                email       text NOT NULL,
                email_lower text GENERATED ALWAYS AS (lower(email)) STORED,
                created_at  timestamp with time zone NOT NULL DEFAULT now(),
                active      boolean NOT NULL DEFAULT true,

                CONSTRAINT pk_users PRIMARY KEY (user_id),
                CONSTRAINT uq_users_email UNIQUE (email),
                CONSTRAINT chk_users_email CHECK (email LIKE '%@%')
            );

            COMMENT ON TABLE {schema_name}.users IS 'Users table';
            COMMENT ON COLUMN {schema_name}.users.email IS 'User email address';

            CREATE INDEX idx_users_name ON {schema_name}.users(name);

            CREATE TABLE {schema_name}.products (
                product_id  integer GENERATED BY DEFAULT AS IDENTITY,
                sku         text NOT NULL,
                price       numeric(10,2) NOT NULL,
                description text NULL,

                CONSTRAINT pk_products PRIMARY KEY (product_id),
                CONSTRAINT uq_products_sku UNIQUE (sku),
                CONSTRAINT chk_products_price CHECK (price >= 0)
            );

            COMMENT ON TABLE {schema_name}.products IS 'Products';

            CREATE TABLE {schema_name}.orders (
                order_id     integer GENERATED BY DEFAULT AS IDENTITY,
                user_id      integer NOT NULL,
                order_number text NOT NULL,
                status       text NOT NULL DEFAULT 'PENDING',
                placed_at    timestamp without time zone NOT NULL DEFAULT now(),
                amount_cents integer NOT NULL,

                CONSTRAINT pk_orders PRIMARY KEY (order_id),
                CONSTRAINT uq_orders_user_number UNIQUE (user_id, order_number),
                CONSTRAINT chk_orders_status CHECK (status IN ('PENDING','PAID','CANCELLED')),

                CONSTRAINT fk_orders_user
                  FOREIGN KEY (user_id) REFERENCES {schema_name}.users(user_id)
                  ON UPDATE CASCADE
                  ON DELETE RESTRICT
            );

            ALTER TABLE {schema_name}.orders
              ADD CONSTRAINT chk_orders_amount CHECK (amount_cents >= 0) NOT VALID;

            CREATE INDEX idx_orders_user_placed_at ON {schema_name}.orders(user_id, placed_at);
            CREATE INDEX idx_orders_paid_recent ON {schema_name}.orders(placed_at) WHERE status = 'PAID';

            CREATE TABLE {schema_name}.order_items (
                order_id          integer NOT NULL,
                product_id        integer NOT NULL,
                line_no           integer NOT NULL,
                quantity          integer NOT NULL,
                unit_price_cents  integer NOT NULL,

                total_amount_cents integer GENERATED ALWAYS AS (quantity * unit_price_cents) STORED,

                CONSTRAINT pk_order_items PRIMARY KEY (order_id, product_id),

                CONSTRAINT fk_oi_order
                  FOREIGN KEY (order_id) REFERENCES {schema_name}.orders(order_id)
                  ON DELETE CASCADE,

                CONSTRAINT fk_oi_product
                  FOREIGN KEY (product_id) REFERENCES {schema_name}.products(product_id)
                  ON DELETE RESTRICT,

                CONSTRAINT chk_oi_quantity CHECK (quantity > 0),
                CONSTRAINT chk_oi_unit_price CHECK (unit_price_cents >= 0)
            );

            CREATE INDEX idx_oi_product ON {schema_name}.order_items(product_id);

            CREATE VIEW {schema_name}.view_paid_orders AS
            SELECT order_id, user_id, placed_at, amount_cents
            FROM {schema_name}.orders
            WHERE status = 'PAID';

            CREATE MATERIALIZED VIEW {schema_name}.revenue_by_day AS
            SELECT
              date_trunc('day', placed_at)::date AS day,
              sum(amount_cents)::bigint          AS total_amount_cents
            FROM {schema_name}.orders
            GROUP BY 1;

            CREATE TABLE {schema_name}.test_partitions (
                id int NOT NULL,
                name varchar(255) NULL
            ) PARTITION BY RANGE (id);

            CREATE TABLE {schema_name}.test_partitions_1 PARTITION OF {schema_name}.test_partitions
            FOR VALUES FROM (0) TO (10);

            CREATE TABLE {schema_name}.test_partitions_2 PARTITION OF {schema_name}.test_partitions
            FOR VALUES FROM (10) TO (20);

            CREATE EXTENSION IF NOT EXISTS file_fdw;
            DROP SERVER IF EXISTS file_server CASCADE;
            CREATE SERVER file_server FOREIGN DATA WRAPPER file_fdw;

            CREATE FOREIGN TABLE {schema_name}.customers_file (
                customer_id  integer,
                email        text,
                full_name    text,
                country_code char(2),
                created_at   timestamp without time zone
            )
            SERVER file_server
            OPTIONS (
                filename '/tmp/customers.csv',
                format 'csv',
                header 'true'
            );

            COMMENT ON FOREIGN TABLE {schema_name}.customers_file IS 'External file-backed table (CSV)';
            """,
    )


def test_postgres_introspection_contract(create_db_schema, postgres_container: PostgresContainer):
    schema_name = "custom"
    with create_db_schema(schema_name):
        _init_with_demo_schema(postgres_container, schema_name)

        plugin = PostgresqlDbPlugin()
        config_file = _create_config_file_from_container(postgres_container)
        execution_result = execute_datasource_plugin(
            plugin, DatasourceType(full_type=config_file["type"]), config_file, "file_name"
        )
        assert isinstance(execution_result.result, DatabaseIntrospectionResult)

        assert_contract(
            execution_result.result,
            [
                TableExists("test", schema_name, "users"),
                TableKindIs("test", schema_name, "users", "table"),
                TableDescriptionContains("test", schema_name, "users", "Users table"),
                ColumnIs("test", schema_name, "users", "user_id", type="integer", generated="identity"),
                ColumnIs("test", schema_name, "users", "name", type="text", nullable=False),
                ColumnIs(
                    "test",
                    schema_name,
                    "users",
                    "email",
                    type="text",
                    nullable=False,
                    description_contains="User email address",
                ),
                ColumnIs(
                    "test",
                    schema_name,
                    "users",
                    "email_lower",
                    type="text",
                    generated="computed",
                    default_contains="lower",
                ),
                ColumnIs(
                    "test", schema_name, "users", "created_at", type="timestamp with time zone", default_contains="now"
                ),
                ColumnIs("test", schema_name, "users", "active", type="boolean", default_contains="true"),
                PrimaryKeyIs("test", schema_name, "users", ["user_id"], name="pk_users"),
                UniqueConstraintExists("test", schema_name, "users", ["email"], name="uq_users_email"),
                CheckConstraintExists("test", schema_name, "users", name="chk_users_email"),
                IndexExists("test", schema_name, "users", name="idx_users_name", columns=["name"]),
                TableExists("test", schema_name, "products"),
                TableKindIs("test", schema_name, "products", "table"),
                TableDescriptionContains("test", schema_name, "products", "Products"),
                ColumnIs("test", schema_name, "products", "product_id", type="integer", generated="identity"),
                ColumnIs("test", schema_name, "products", "sku", type="text", nullable=False),
                ColumnIs("test", schema_name, "products", "price", type="numeric(10,2)", nullable=False),
                PrimaryKeyIs("test", schema_name, "products", ["product_id"], name="pk_products"),
                UniqueConstraintExists("test", schema_name, "products", ["sku"], name="uq_products_sku"),
                CheckConstraintExists("test", schema_name, "products", name="chk_products_price"),
                TableExists("test", schema_name, "orders"),
                TableKindIs("test", schema_name, "orders", "table"),
                ColumnIs("test", schema_name, "orders", "order_id", type="integer", generated="identity"),
                ColumnIs("test", schema_name, "orders", "status", type="text", default_contains="PENDING"),
                ColumnIs(
                    "test",
                    schema_name,
                    "orders",
                    "placed_at",
                    type="timestamp without time zone",
                    default_contains="now",
                ),
                PrimaryKeyIs("test", schema_name, "orders", ["order_id"], name="pk_orders"),
                UniqueConstraintExists(
                    "test", schema_name, "orders", ["user_id", "order_number"], name="uq_orders_user_number"
                ),
                ForeignKeyExists(
                    "test",
                    schema_name,
                    "orders",
                    name="fk_orders_user",
                    from_columns=["user_id"],
                    ref_table=f"{schema_name}.users",
                    ref_columns=["user_id"],
                ),
                CheckConstraintExists("test", schema_name, "orders", name="chk_orders_status"),
                CheckConstraintExists("test", schema_name, "orders", name="chk_orders_amount", validated=False),
                IndexExists(
                    "test", schema_name, "orders", name="idx_orders_user_placed_at", columns=["user_id", "placed_at"]
                ),
                IndexExists(
                    "test",
                    schema_name,
                    "orders",
                    name="idx_orders_paid_recent",
                    columns=["placed_at"],
                    predicate_contains="PAID",
                ),
                TableExists("test", schema_name, "order_items"),
                TableKindIs("test", schema_name, "order_items", "table"),
                ColumnIs(
                    "test",
                    schema_name,
                    "order_items",
                    "total_amount_cents",
                    generated="computed",
                    default_contains="unit_price_cents",
                ),
                PrimaryKeyIs("test", schema_name, "order_items", ["order_id", "product_id"], name="pk_order_items"),
                ForeignKeyExists(
                    "test",
                    schema_name,
                    "order_items",
                    name="fk_oi_order",
                    from_columns=["order_id"],
                    ref_table=f"{schema_name}.orders",
                    ref_columns=["order_id"],
                ),
                ForeignKeyExists(
                    "test",
                    schema_name,
                    "order_items",
                    name="fk_oi_product",
                    from_columns=["product_id"],
                    ref_table=f"{schema_name}.products",
                    ref_columns=["product_id"],
                ),
                CheckConstraintExists("test", schema_name, "order_items", name="chk_oi_quantity"),
                CheckConstraintExists("test", schema_name, "order_items", name="chk_oi_unit_price"),
                IndexExists("test", schema_name, "order_items", name="idx_oi_product", columns=["product_id"]),
                TableExists("test", schema_name, "view_paid_orders"),
                TableKindIs("test", schema_name, "view_paid_orders", "view"),
                TableExists("test", schema_name, "revenue_by_day"),
                TableKindIs("test", schema_name, "revenue_by_day", "materialized_view"),
                ColumnIs("test", schema_name, "revenue_by_day", "day", type="date"),
                ColumnIs("test", schema_name, "revenue_by_day", "total_amount_cents", type="bigint"),
                TableExists("test", schema_name, "test_partitions"),
                TableKindIs("test", schema_name, "test_partitions", "table"),
                ColumnIs("test", schema_name, "test_partitions", "id", type="integer", nullable=False),
                ColumnIs("test", schema_name, "test_partitions", "name", type="character varying(255)", nullable=True),
                PartitionMetaContains(
                    "test",
                    schema_name,
                    "test_partitions",
                    expected_meta={
                        "columns_in_partition_key": ["id"],
                        "partitioning_strategy": "range partitioned",
                    },
                ),
                TableExists("test", schema_name, "customers_file"),
                TableKindIs("test", schema_name, "customers_file", "external_table"),
                TableDescriptionContains("test", schema_name, "customers_file", "External file-backed table"),
                ColumnIs("test", schema_name, "customers_file", "customer_id", type="integer"),
                ColumnIs("test", schema_name, "customers_file", "country_code", type="character(2)"),
            ],
        )


def _create_config_file_from_container(postgres_container_with_columns: PostgresContainer) -> Mapping[str, Any]:
    return {
        "type": "databases/postgres",
        "connection": {
            "host": postgres_container_with_columns.get_container_host_ip(),
            "port": postgres_container_with_columns.get_exposed_port(postgres_container_with_columns.port),
            "database": postgres_container_with_columns.dbname,
            "user": postgres_container_with_columns.username,
            "password": postgres_container_with_columns.password,
        },
    }
