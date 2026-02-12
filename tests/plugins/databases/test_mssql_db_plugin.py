import contextlib
import platform
from typing import Any, Mapping, Sequence

import mssql_python  # type: ignore
import pytest
from testcontainers.mssql import SqlServerContainer  # type: ignore

from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.pluginlib.plugin_utils import execute_datasource_plugin
from databao_context_engine.plugins.databases.databases_types import DatabaseIntrospectionResult
from databao_context_engine.plugins.databases.mssql.mssql_db_plugin import MSSQLDbPlugin
from tests.plugins.databases.database_contracts import (
    CheckConstraintExists,
    ColumnIs,
    ForeignKeyExists,
    IndexExists,
    PrimaryKeyIs,
    SamplesCountIs,
    SamplesEqual,
    TableDescriptionContains,
    TableExists,
    TableKindIs,
    UniqueConstraintExists,
    assert_contract,
)

MSSQL_HTTP_PORT = 1433
# doesn't work with mssql_container.get_container_host_ip (localhost)
MSSQL_HOST = "127.0.0.1"


def _is_nixos_distro() -> bool:
    try:
        os_release = platform.freedesktop_os_release()
        release_name = os_release["NAME"]
        return "nixos" in release_name.lower()
    except (OSError, KeyError):
        return False


@pytest.fixture(scope="module")
def mssql_container():
    if _is_nixos_distro():
        pytest.skip("mssql-python connector doesn't work on NixOS out of the box")

    container = SqlServerContainer()
    container.start()
    yield container
    container.stop()


@pytest.fixture
def create_mssql_conn(mssql_container: SqlServerContainer):
    def _create_connection():
        port = mssql_container.get_exposed_port(MSSQL_HTTP_PORT)
        connection_parts = {
            "server": f"{MSSQL_HOST},{port}",
            "database": mssql_container.dbname,
            "uid": mssql_container.username,
            "pwd": mssql_container.password,
            "encrypt": "no",
        }
        connection_string = ";".join(f"{k}={v}" for k, v in connection_parts.items() if v is not None)
        return mssql_python.connect(connection_string, autocommit=True)

    return _create_connection


@contextlib.contextmanager
def seed_rows(
    create_mssql_conn,
    catalog: str,
    full_table_name: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    cleanup_sql: Sequence[str] = (),
    reseed_identity: bool = True,
    identity_col: str = "id",
):
    conn = create_mssql_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"USE {catalog};")

            for stmt in cleanup_sql:
                cursor.execute(stmt)
            if not cleanup_sql:
                cursor.execute(f"DELETE FROM {full_table_name};")

            if rows:
                columns = list(rows[0].keys())

                wants_identity_insert = identity_col in columns
                if wants_identity_insert:
                    cursor.execute(f"SET IDENTITY_INSERT {full_table_name} ON;")

                col_sql = ", ".join(columns)
                placeholders = ", ".join(["?"] * len(columns))
                sql = f"INSERT INTO {full_table_name} ({col_sql}) VALUES ({placeholders});"

                for r in rows:
                    cursor.execute(sql, tuple(r[c] for c in columns))

                if wants_identity_insert:
                    cursor.execute(f"SET IDENTITY_INSERT {full_table_name} OFF;")

                if reseed_identity and wants_identity_insert:
                    max_id = max(int(r[identity_col]) for r in rows)
                    cursor.execute(f"DBCC CHECKIDENT ('{full_table_name}', RESEED, {max_id});")

        yield
    finally:
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"USE {catalog};")
                for stmt in cleanup_sql:
                    cursor.execute(stmt)
                if not cleanup_sql:
                    cursor.execute(f"DELETE FROM {full_table_name};")
        finally:
            conn.close()


@pytest.fixture(scope="module")
def mssql_container_with_demo_schema(mssql_container: SqlServerContainer):
    port = mssql_container.get_exposed_port(MSSQL_HTTP_PORT)
    connection_parts = {
        "server": f"{MSSQL_HOST},{port}",
        "database": mssql_container.dbname,
        "uid": mssql_container.username,
        "pwd": mssql_container.password,
        "encrypt": "no",
    }
    connection_string = ";".join(f"{k}={v}" for k, v in connection_parts.items() if v is not None)

    conn = mssql_python.connect(connection_string, autocommit=True)

    try:
        with conn.cursor() as cursor:
            cursor.execute("CREATE DATABASE catalog_main;")
            cursor.execute("USE catalog_main;")

            cursor.execute("DROP VIEW IF EXISTS dbo.view_paid_orders;")
            cursor.execute("DROP TABLE IF EXISTS dbo.table_order_items;")
            cursor.execute("DROP TABLE IF EXISTS dbo.table_orders;")
            cursor.execute("DROP TABLE IF EXISTS dbo.table_products;")
            cursor.execute("DROP TABLE IF EXISTS dbo.table_users;")

            cursor.execute("""
                CREATE TABLE dbo.table_users
                (
                    id          INT IDENTITY(1,1) NOT NULL,
                    name        VARCHAR(255) NOT NULL,
                    email       VARCHAR(255) NOT NULL,
                    email_lower AS (lower([email])) PERSISTED,
                    created_at  DATETIME2(0) NOT NULL CONSTRAINT df_table_users_created_at DEFAULT (sysutcdatetime()),
                    active      BIT NOT NULL CONSTRAINT df_table_users_active DEFAULT (1),
    
                    CONSTRAINT pk_table_users PRIMARY KEY (id),
                    CONSTRAINT uq_table_users_email UNIQUE (email),
                    CONSTRAINT chk_table_users_email CHECK (email LIKE '%@%')
                );
            """)

            cursor.execute("CREATE INDEX ix_table_users_name ON dbo.table_users(name);")

            cursor.execute("""
                EXEC sys.sp_addextendedproperty
                    @name = N'MS_Description',
                    @value = N'Users table',
                    @level0type = N'SCHEMA', @level0name = N'dbo',
                    @level1type = N'TABLE',  @level1name = N'table_users';
            """)
            cursor.execute("""
                EXEC sys.sp_addextendedproperty
                    @name = N'MS_Description',
                    @value = N'User email address',
                    @level0type = N'SCHEMA', @level0name = N'dbo',
                    @level1type = N'TABLE',  @level1name = N'table_users',
                    @level2type = N'COLUMN', @level2name = N'email';
            """)

            cursor.execute("""
                CREATE TABLE dbo.table_products
                (
                    id          INT IDENTITY(1,1) NOT NULL,
                    sku         VARCHAR(32) NOT NULL,
                    price       DECIMAL(10,2) NOT NULL,
                    description NVARCHAR(MAX) NULL,
    
                    CONSTRAINT pk_table_products PRIMARY KEY (id),
                    CONSTRAINT uq_table_products_sku UNIQUE (sku),
                    CONSTRAINT chk_table_products_price CHECK (price >= 0)
                );
            """)

            cursor.execute("""
                CREATE TABLE dbo.table_orders
                (
                    id                INT IDENTITY(1000,1) NOT NULL,
                    user_id            INT NOT NULL,
                    order_number       VARCHAR(64) NOT NULL,
                    order_number_lower AS (lower([order_number])) PERSISTED,
    
                    status    NVARCHAR(16) NOT NULL CONSTRAINT df_table_orders_status DEFAULT (N'PENDING'),
                    placed_at DATETIME2(0) NOT NULL CONSTRAINT df_table_orders_placed_at DEFAULT (sysutcdatetime()),
                    amount_cents INT NOT NULL,
    
                    CONSTRAINT pk_table_orders PRIMARY KEY (id),
                    CONSTRAINT uq_table_orders_user_number UNIQUE (user_id, order_number),
    
                    CONSTRAINT chk_table_orders_status CHECK (status IN (N'PENDING', N'PAID', N'CANCELLED')),
                    CONSTRAINT chk_table_orders_amount CHECK (amount_cents >= 0),
    
                    CONSTRAINT fk_orders_user
                        FOREIGN KEY (user_id) REFERENCES dbo.table_users(id)
                        ON UPDATE CASCADE
                        ON DELETE NO ACTION
                );
            """)

            cursor.execute("CREATE INDEX ix_table_orders_user_placed_at ON dbo.table_orders(user_id, placed_at);")
            cursor.execute("CREATE INDEX ix_orders_paid_recent ON dbo.table_orders(placed_at) WHERE status = N'PAID';")

            cursor.execute("""
                CREATE TABLE dbo.table_order_items
                (
                    order_id          INT NOT NULL,
                    product_id        INT NOT NULL,
                    quantity          INT NOT NULL,
                    unit_price_cents  INT NOT NULL,
                    total_amount_cents AS (quantity * unit_price_cents) PERSISTED,
    
                    CONSTRAINT pk_table_order_items PRIMARY KEY (order_id, product_id),
    
                    CONSTRAINT fk_oi_order
                        FOREIGN KEY (order_id) REFERENCES dbo.table_orders(id)
                        ON DELETE CASCADE,
    
                    CONSTRAINT fk_oi_product
                        FOREIGN KEY (product_id) REFERENCES dbo.table_products(id),
    
                    CONSTRAINT chk_oi_qty CHECK (quantity > 0),
                    CONSTRAINT chk_oi_price CHECK (unit_price_cents >= 0)
                );
            """)

            cursor.execute("CREATE INDEX ix_oi_product ON dbo.table_order_items(product_id);")

            cursor.execute("""
                CREATE VIEW dbo.view_paid_orders AS
                SELECT
                    id AS order_id,
                    user_id,
                    placed_at,
                    amount_cents
                FROM dbo.table_orders
                WHERE status = N'PAID';
            """)

            cursor.execute("CREATE DATABASE catalog_aux;")
            cursor.execute("USE catalog_aux;")

            cursor.execute("DROP VIEW IF EXISTS dbo.active_employees;")
            cursor.execute("DROP TABLE IF EXISTS dbo.employees;")
            cursor.execute("DROP TABLE IF EXISTS dbo.departments;")

            cursor.execute("""
                CREATE TABLE dbo.departments
                (
                    id   INT IDENTITY(1,1) NOT NULL,
                    name NVARCHAR(100) NOT NULL,
    
                    CONSTRAINT pk_departments PRIMARY KEY (id),
                    CONSTRAINT uq_departments_name UNIQUE (name)
                );
            """)

            cursor.execute("""
                CREATE TABLE dbo.employees
                (
                    id            INT IDENTITY(1,1) NOT NULL,
                    department_id INT NULL,
                    email         NVARCHAR(255) NOT NULL,
                    salary_cents  INT NOT NULL,
                    active        BIT NOT NULL CONSTRAINT df_employees_active DEFAULT (1),
                    hired_at      DATETIME2(0) NOT NULL CONSTRAINT df_employees_hired_at DEFAULT (sysutcdatetime()),
    
                    CONSTRAINT pk_employees PRIMARY KEY (id),
                    CONSTRAINT uq_employees_email UNIQUE (email),
                    CONSTRAINT chk_employees_salary CHECK (salary_cents >= 0),
    
                    CONSTRAINT fk_employees_department
                        FOREIGN KEY (department_id) REFERENCES dbo.departments(id)
                        ON DELETE SET NULL
                );
            """)

            cursor.execute("CREATE INDEX ix_employees_dept ON dbo.employees(department_id);")

            cursor.execute("""
                CREATE VIEW dbo.active_employees AS
                SELECT id, department_id, email, hired_at
                FROM dbo.employees
                WHERE active = 1;
            """)
    finally:
        cursor.close()
        conn.close()

    return mssql_container


def test_mssql_introspection(mssql_container_with_demo_schema):
    plugin = MSSQLDbPlugin()
    config_file = _create_config_file_from_container(mssql_container_with_demo_schema)
    result = execute_datasource_plugin(plugin, DatasourceType(full_type=config_file["type"]), config_file, "file_name")
    assert isinstance(result, DatabaseIntrospectionResult)

    assert_contract(
        result,
        [
            TableExists("catalog_main", "dbo", "table_users"),
            TableKindIs("catalog_main", "dbo", "table_users", "table"),
            TableDescriptionContains("catalog_main", "dbo", "table_users", "Users table"),
            ColumnIs("catalog_main", "dbo", "table_users", "id", type="int", nullable=False, generated="identity"),
            ColumnIs(
                "catalog_main",
                "dbo",
                "table_users",
                "email",
                type="varchar(255)",
                nullable=False,
                description_contains="User email address",
            ),
            ColumnIs(
                "catalog_main",
                "dbo",
                "table_users",
                "email_lower",
                type="varchar(255)",
                generated="computed",
                default_contains="lower",
            ),
            ColumnIs(
                "catalog_main",
                "dbo",
                "table_users",
                "created_at",
                type="datetime2",
                nullable=False,
                default_contains="sysutcdatetime",
            ),
            ColumnIs("catalog_main", "dbo", "table_users", "active", type="bit", nullable=False, default_contains="1"),
            PrimaryKeyIs("catalog_main", "dbo", "table_users", ["id"], name="pk_table_users"),
            UniqueConstraintExists("catalog_main", "dbo", "table_users", ["email"], name="uq_table_users_email"),
            CheckConstraintExists("catalog_main", "dbo", "table_users", name="chk_table_users_email"),
            IndexExists("catalog_main", "dbo", "table_users", name="ix_table_users_name", columns=["name"]),
            TableExists("catalog_main", "dbo", "table_products"),
            TableKindIs("catalog_main", "dbo", "table_products", "table"),
            ColumnIs("catalog_main", "dbo", "table_products", "id", type="int", nullable=False, generated="identity"),
            ColumnIs("catalog_main", "dbo", "table_products", "price", type="decimal(10,2)", nullable=False),
            ColumnIs("catalog_main", "dbo", "table_products", "description", type="nvarchar(MAX)", nullable=True),
            UniqueConstraintExists("catalog_main", "dbo", "table_products", ["sku"], name="uq_table_products_sku"),
            CheckConstraintExists("catalog_main", "dbo", "table_products", name="chk_table_products_price"),
            TableExists("catalog_main", "dbo", "table_orders"),
            TableKindIs("catalog_main", "dbo", "table_orders", "table"),
            ColumnIs("catalog_main", "dbo", "table_orders", "id", type="int", nullable=False, generated="identity"),
            ColumnIs(
                "catalog_main",
                "dbo",
                "table_orders",
                "status",
                type="nvarchar(16)",
                nullable=False,
                default_contains="PENDING",
            ),
            ColumnIs(
                "catalog_main",
                "dbo",
                "table_orders",
                "placed_at",
                type="datetime2",
                nullable=False,
                default_contains="sysutcdatetime",
            ),
            CheckConstraintExists("catalog_main", "dbo", "table_orders", name="chk_table_orders_status"),
            CheckConstraintExists("catalog_main", "dbo", "table_orders", name="chk_table_orders_amount"),
            ForeignKeyExists(
                "catalog_main",
                "dbo",
                "table_orders",
                name="fk_orders_user",
                from_columns=["user_id"],
                ref_table="dbo.table_users",
                ref_columns=["id"],
            ),
            IndexExists(
                "catalog_main",
                "dbo",
                "table_orders",
                name="ix_table_orders_user_placed_at",
                columns=["user_id", "placed_at"],
            ),
            IndexExists(
                "catalog_main",
                "dbo",
                "table_orders",
                name="ix_orders_paid_recent",
                columns=["placed_at"],
                predicate_contains="PAID",
            ),
            TableExists("catalog_main", "dbo", "table_order_items"),
            TableKindIs("catalog_main", "dbo", "table_order_items", "table"),
            ColumnIs(
                "catalog_main",
                "dbo",
                "table_order_items",
                "total_amount_cents",
                generated="computed",
                default_contains="unit_price_cents",
            ),
            PrimaryKeyIs(
                "catalog_main", "dbo", "table_order_items", ["order_id", "product_id"], name="pk_table_order_items"
            ),
            CheckConstraintExists("catalog_main", "dbo", "table_order_items", name="chk_oi_qty"),
            CheckConstraintExists("catalog_main", "dbo", "table_order_items", name="chk_oi_price"),
            ForeignKeyExists(
                "catalog_main",
                "dbo",
                "table_order_items",
                name="fk_oi_order",
                from_columns=["order_id"],
                ref_table="dbo.table_orders",
                ref_columns=["id"],
            ),
            ForeignKeyExists(
                "catalog_main",
                "dbo",
                "table_order_items",
                name="fk_oi_product",
                from_columns=["product_id"],
                ref_table="dbo.table_products",
                ref_columns=["id"],
            ),
            IndexExists("catalog_main", "dbo", "table_order_items", name="ix_oi_product", columns=["product_id"]),
            TableExists("catalog_main", "dbo", "view_paid_orders"),
            TableKindIs("catalog_main", "dbo", "view_paid_orders", "view"),
            ColumnIs("catalog_main", "dbo", "view_paid_orders", "order_id", type="int"),
            ColumnIs("catalog_main", "dbo", "view_paid_orders", "placed_at", type="datetime2"),
            TableExists("catalog_aux", "dbo", "departments"),
            TableKindIs("catalog_aux", "dbo", "departments", "table"),
            UniqueConstraintExists("catalog_aux", "dbo", "departments", ["name"], name="uq_departments_name"),
            TableExists("catalog_aux", "dbo", "employees"),
            TableKindIs("catalog_aux", "dbo", "employees", "table"),
            ColumnIs("catalog_aux", "dbo", "employees", "active", type="bit", nullable=False, default_contains="1"),
            ColumnIs(
                "catalog_aux",
                "dbo",
                "employees",
                "hired_at",
                type="datetime2",
                nullable=False,
                default_contains="sysutcdatetime",
            ),
            CheckConstraintExists("catalog_aux", "dbo", "employees", name="chk_employees_salary"),
            ForeignKeyExists(
                "catalog_aux",
                "dbo",
                "employees",
                name="fk_employees_department",
                from_columns=["department_id"],
                ref_table="dbo.departments",
                ref_columns=["id"],
            ),
            IndexExists("catalog_aux", "dbo", "employees", name="ix_employees_dept", columns=["department_id"]),
            TableExists("catalog_aux", "dbo", "active_employees"),
            TableKindIs("catalog_aux", "dbo", "active_employees", "view"),
        ],
    )


def test_mssql_exact_samples(mssql_container_with_demo_schema, create_mssql_conn):
    rows = [
        {"id": 1, "sku": "SKU-1", "price": 10.50, "description": "foo"},
        {"id": 2, "sku": "SKU-2", "price": 20.00, "description": None},
    ]

    cleanup = [
        "DELETE FROM dbo.table_order_items;",
        "DELETE FROM dbo.table_products;",
    ]

    with seed_rows(create_mssql_conn, "catalog_main", "dbo.table_products", rows, cleanup_sql=cleanup):
        plugin = MSSQLDbPlugin()
        config_file = _create_config_file_from_container(mssql_container_with_demo_schema)
        result = execute_datasource_plugin(
            plugin, DatasourceType(full_type=config_file["type"]), config_file, "file_name"
        )
        assert isinstance(result, DatabaseIntrospectionResult)

        assert_contract(
            result,
            [
                TableExists("catalog_main", "dbo", "table_products"),
                SamplesEqual("catalog_main", "dbo", "table_products", rows=rows),
            ],
        )


def test_mssql_samples_in_big(mssql_container_with_demo_schema, create_mssql_conn):
    plugin = MSSQLDbPlugin()
    limit = plugin._introspector._SAMPLE_LIMIT

    rows = [{"id": i, "sku": f"SKU-{i}", "price": float(i), "description": None} for i in range(1, 1000)]

    cleanup = [
        "DELETE FROM dbo.table_order_items;",
        "DELETE FROM dbo.table_products;",
    ]

    with seed_rows(create_mssql_conn, "catalog_main", "dbo.table_products", rows, cleanup_sql=cleanup):
        config_file = _create_config_file_from_container(mssql_container_with_demo_schema)
        result = execute_datasource_plugin(
            plugin, DatasourceType(full_type=config_file["type"]), config_file, "file_name"
        )
        assert isinstance(result, DatabaseIntrospectionResult)

        assert_contract(
            result,
            [
                TableExists("catalog_main", "dbo", "table_products"),
                SamplesCountIs("catalog_main", "dbo", "table_products", count=limit),
            ],
        )


def _create_config_file_from_container(
    mssql: SqlServerContainer, datasource_name: str | None = "file_name"
) -> Mapping[str, Any]:
    return {
        "name": datasource_name,
        "type": "databases/mssql",
        "connection": {
            "host": MSSQL_HOST,
            "port": int(mssql.get_exposed_port(MSSQL_HTTP_PORT)),
            "user": mssql.username,
            "password": mssql.password,
            "encrypt": "no",
        },
    }
