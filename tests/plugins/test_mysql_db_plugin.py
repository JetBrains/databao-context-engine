import contextlib
from typing import Any, Mapping, Sequence

import pymysql
import pytest
from testcontainers.mysql import MySqlContainer  # type: ignore

from nemory.pluginlib.build_plugin import DatasourceType
from nemory.pluginlib.plugin_utils import execute_datasource_plugin
from nemory.plugins.databases.databases_types import DatabaseIntrospectionResult
from nemory.plugins.mysql_db_plugin import MySQLDbPlugin
from tests.plugins.database_contracts import (
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


@pytest.fixture(scope="module")
def mysql_container():
    container = MySqlContainer()
    container.start()
    yield container
    container.stop()


@pytest.fixture
def create_mysql_conn(mysql_container: MySqlContainer):
    def _create_connection():
        return pymysql.connect(
            host=mysql_container.get_container_host_ip(),
            port=int(mysql_container.get_exposed_port(mysql_container.port)),
            user="root",
            password=mysql_container.root_password,
            database=mysql_container.dbname,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )

    return _create_connection


@contextlib.contextmanager
def seed_rows(
    create_mysql_conn,
    catalog: str,
    table: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    cleanup_sql: Sequence[str] = (),
):
    conn = create_mysql_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"USE {catalog};")

            if cleanup_sql:
                for stmt in cleanup_sql:
                    cursor.execute(stmt)
            else:
                cursor.execute(f"DELETE FROM {table};")

            if rows:
                columns = list(rows[0].keys())

                col_sql = ", ".join(columns)
                placeholders = ", ".join(["%s"] * len(columns))
                sql = f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})"

                data = [tuple(r[c] for c in columns) for r in rows]
                cursor.executemany(sql, data)

        yield
    finally:
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"USE {catalog};")
                if cleanup_sql:
                    for stmt in cleanup_sql:
                        cursor.execute(stmt)
                else:
                    cursor.execute(f"DELETE FROM {table};")
        finally:
            conn.close()


@pytest.fixture(scope="module")
def mysql_container_with_demo_schema(mysql_container: MySqlContainer):
    conn = pymysql.connect(
        host=mysql_container.get_container_host_ip(),
        port=int(mysql_container.get_exposed_port(mysql_container.port)),
        user="root",
        password=mysql_container.root_password,
        database=mysql_container.dbname,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute("CREATE DATABASE IF NOT EXISTS catalog_main;")
            cursor.execute("CREATE DATABASE IF NOT EXISTS catalog_aux;")

            cursor.execute(f"GRANT ALL PRIVILEGES ON *.* TO '{mysql_container.username}'@'%';")
            cursor.execute("FLUSH PRIVILEGES;")

            cursor.execute("USE catalog_main;")

            cursor.execute("DROP VIEW IF EXISTS view_paid_orders;")
            cursor.execute("DROP TABLE IF EXISTS table_order_items;")
            cursor.execute("DROP TABLE IF EXISTS table_orders;")
            cursor.execute("DROP TABLE IF EXISTS table_products;")
            cursor.execute("DROP TABLE IF EXISTS table_users;")

            cursor.execute("""
                CREATE TABLE table_users (
                  id INT NOT NULL AUTO_INCREMENT COMMENT 'surrogate user id',
                  name VARCHAR(255) NOT NULL,
                  email VARCHAR(255) NOT NULL COMMENT 'user email',
                  email_lower VARCHAR(255) GENERATED ALWAYS AS (LOWER(email)) STORED COMMENT 'lowercased email',
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'creation time',

                  CONSTRAINT uq_table_users_email UNIQUE (email),
                  CONSTRAINT chk_table_users_email CHECK (email LIKE '%@%'),

                  PRIMARY KEY (id),
                  INDEX idx_table_users_name (name)
                ) ENGINE=InnoDB COMMENT='Users';
            """)

            cursor.execute("""
                CREATE TABLE table_products (
                  id INT NOT NULL AUTO_INCREMENT,
                  sku VARCHAR(32) NOT NULL,
                  price DECIMAL(10,2) NOT NULL,
                  description TEXT NULL,

                  CONSTRAINT uq_table_products_sku UNIQUE (sku),
                  CONSTRAINT chk_table_products_price CHECK (price >= 0),

                  PRIMARY KEY (id)
                ) ENGINE=InnoDB COMMENT='Products';
            """)

            cursor.execute("""
                CREATE TABLE table_orders (
                  id INT NOT NULL AUTO_INCREMENT,
                  user_id INT NOT NULL,
                  order_number VARCHAR(64) NOT NULL,
                  status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
                  placed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

                  CONSTRAINT uq_table_orders_user_number UNIQUE (user_id, order_number),
                  CONSTRAINT chk_table_orders_status CHECK (status IN ('PENDING','PAID','CANCELLED')),

                  CONSTRAINT fk_orders_user
                    FOREIGN KEY (user_id) REFERENCES table_users(id)
                    ON DELETE RESTRICT
                    ON UPDATE CASCADE,

                  PRIMARY KEY (id),
                  INDEX idx_orders_user_placed_at (user_id, placed_at)
                ) ENGINE=InnoDB COMMENT='Orders';
            """)

            cursor.execute("""
                CREATE TABLE table_order_items (
                  order_id INT NOT NULL,
                  product_id INT NOT NULL,
                  quantity INT NOT NULL,
                  unit_price_cents INT NOT NULL,
                  total_amount_cents INT GENERATED ALWAYS AS (quantity * unit_price_cents) STORED,

                  CONSTRAINT chk_oi_quantity CHECK (quantity > 0),
                  CONSTRAINT chk_oi_unit_price CHECK (unit_price_cents >= 0),

                  CONSTRAINT fk_oi_order
                    FOREIGN KEY (order_id) REFERENCES table_orders(id)
                    ON DELETE CASCADE
                    ON UPDATE RESTRICT,

                  CONSTRAINT fk_oi_product
                    FOREIGN KEY (product_id) REFERENCES table_products(id)
                    ON DELETE RESTRICT
                    ON UPDATE RESTRICT,

                  PRIMARY KEY (order_id, product_id),
                  INDEX idx_oi_product (product_id)
                ) ENGINE=InnoDB COMMENT='Order items';
            """)

            cursor.execute("""
                CREATE VIEW view_paid_orders AS
                SELECT
                  o.id AS order_id,
                  o.user_id,
                  o.placed_at
                FROM table_orders o
                WHERE o.status = 'PAID';
            """)

            cursor.execute("USE catalog_aux;")

            cursor.execute("DROP VIEW IF EXISTS active_employees;")
            cursor.execute("DROP TABLE IF EXISTS employees;")
            cursor.execute("DROP TABLE IF EXISTS departments;")

            cursor.execute("""
                CREATE TABLE departments (
                  id INT NOT NULL AUTO_INCREMENT,
                  name VARCHAR(100) NOT NULL,
                  CONSTRAINT uq_departments_name UNIQUE (name),
                  PRIMARY KEY (id)
                ) ENGINE=InnoDB COMMENT='Departments';
            """)

            cursor.execute("""
                CREATE TABLE employees (
                  id INT NOT NULL AUTO_INCREMENT,
                  department_id INT NULL,
                  email VARCHAR(255) NOT NULL,
                  salary_cents INT NOT NULL,
                  active TINYINT(1) NOT NULL DEFAULT 1,
                  hired_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

                  CONSTRAINT uq_employees_email UNIQUE (email),
                  CONSTRAINT chk_employees_salary CHECK (salary_cents >= 0),

                  CONSTRAINT fk_employees_department
                    FOREIGN KEY (department_id) REFERENCES departments(id)
                    ON DELETE SET NULL
                    ON UPDATE RESTRICT,

                  PRIMARY KEY (id),
                  INDEX idx_employees_dept (department_id)
                ) ENGINE=InnoDB COMMENT='Employees';
            """)

            cursor.execute("""
                CREATE VIEW active_employees AS
                SELECT id, department_id, email, hired_at
                FROM employees
                WHERE active = 1;
            """)
    finally:
        conn.close()
    return mysql_container


def test_mysql_introspection(mysql_container_with_demo_schema):
    plugin = MySQLDbPlugin()
    config_file = _create_config_file_from_container(mysql_container_with_demo_schema)
    result = execute_datasource_plugin(
        plugin, DatasourceType(full_type=config_file["type"]), config_file, "file_name"
    ).result

    assert_contract(
        result,
        [
            TableExists("catalog_main", "catalog_main", "table_users"),
            TableKindIs("catalog_main", "catalog_main", "table_users", "table"),
            TableDescriptionContains("catalog_main", "catalog_main", "table_users", "Users"),
            ColumnIs(
                "catalog_main",
                "catalog_main",
                "table_users",
                "id",
                type="int",
                nullable=False,
                generated="identity",
                description_contains="surrogate user id",
            ),
            ColumnIs(
                "catalog_main",
                "catalog_main",
                "table_users",
                "email",
                type="varchar(255)",
                nullable=False,
                description_contains="user email",
            ),
            ColumnIs(
                "catalog_main",
                "catalog_main",
                "table_users",
                "email_lower",
                type="varchar(255)",
                generated="computed",
                default_contains="email",
            ),
            ColumnIs(
                "catalog_main",
                "catalog_main",
                "table_users",
                "created_at",
                type="timestamp",
                nullable=False,
                default_contains="CURRENT_TIMESTAMP",
            ),
            PrimaryKeyIs("catalog_main", "catalog_main", "table_users", ["id"]),
            UniqueConstraintExists(
                "catalog_main", "catalog_main", "table_users", ["email"], name="uq_table_users_email"
            ),
            CheckConstraintExists("catalog_main", "catalog_main", "table_users", name="chk_table_users_email"),
            IndexExists("catalog_main", "catalog_main", "table_users", name="idx_table_users_name", columns=["name"]),
            TableExists("catalog_main", "catalog_main", "table_products"),
            TableKindIs("catalog_main", "catalog_main", "table_products", "table"),
            TableDescriptionContains("catalog_main", "catalog_main", "table_products", "Products"),
            ColumnIs(
                "catalog_main", "catalog_main", "table_products", "id", type="int", nullable=False, generated="identity"
            ),
            ColumnIs("catalog_main", "catalog_main", "table_products", "sku", type="varchar(32)", nullable=False),
            ColumnIs("catalog_main", "catalog_main", "table_products", "price", type="decimal(10,2)", nullable=False),
            PrimaryKeyIs("catalog_main", "catalog_main", "table_products", ["id"]),
            UniqueConstraintExists(
                "catalog_main", "catalog_main", "table_products", ["sku"], name="uq_table_products_sku"
            ),
            CheckConstraintExists("catalog_main", "catalog_main", "table_products", name="chk_table_products_price"),
            TableExists("catalog_main", "catalog_main", "table_orders"),
            TableKindIs("catalog_main", "catalog_main", "table_orders", "table"),
            TableDescriptionContains("catalog_main", "catalog_main", "table_orders", "Orders"),
            ColumnIs(
                "catalog_main", "catalog_main", "table_orders", "id", type="int", nullable=False, generated="identity"
            ),
            ColumnIs(
                "catalog_main",
                "catalog_main",
                "table_orders",
                "status",
                type="varchar(16)",
                nullable=False,
                default_contains="PENDING",
            ),
            ColumnIs(
                "catalog_main",
                "catalog_main",
                "table_orders",
                "placed_at",
                type="timestamp",
                nullable=False,
                default_contains="CURRENT_TIMESTAMP",
            ),
            PrimaryKeyIs("catalog_main", "catalog_main", "table_orders", ["id"]),
            UniqueConstraintExists(
                "catalog_main",
                "catalog_main",
                "table_orders",
                ["user_id", "order_number"],
                name="uq_table_orders_user_number",
            ),
            CheckConstraintExists("catalog_main", "catalog_main", "table_orders", name="chk_table_orders_status"),
            ForeignKeyExists(
                "catalog_main",
                "catalog_main",
                "table_orders",
                from_columns=["user_id"],
                ref_table="catalog_main.table_users",
                ref_columns=["id"],
                name="fk_orders_user",
                on_delete="restrict",
                on_update="cascade",
            ),
            IndexExists(
                "catalog_main",
                "catalog_main",
                "table_orders",
                name="idx_orders_user_placed_at",
                columns=["user_id", "placed_at"],
            ),
            TableExists("catalog_main", "catalog_main", "table_order_items"),
            TableKindIs("catalog_main", "catalog_main", "table_order_items", "table"),
            TableDescriptionContains("catalog_main", "catalog_main", "table_order_items", "Order items"),
            ColumnIs(
                "catalog_main",
                "catalog_main",
                "table_order_items",
                "total_amount_cents",
                type="int",
                generated="computed",
                default_contains="quantity",
            ),
            PrimaryKeyIs("catalog_main", "catalog_main", "table_order_items", ["order_id", "product_id"]),
            CheckConstraintExists("catalog_main", "catalog_main", "table_order_items", name="chk_oi_quantity"),
            CheckConstraintExists("catalog_main", "catalog_main", "table_order_items", name="chk_oi_unit_price"),
            ForeignKeyExists(
                "catalog_main",
                "catalog_main",
                "table_order_items",
                from_columns=["order_id"],
                ref_table="catalog_main.table_orders",
                ref_columns=["id"],
                name="fk_oi_order",
            ),
            ForeignKeyExists(
                "catalog_main",
                "catalog_main",
                "table_order_items",
                from_columns=["product_id"],
                ref_table="catalog_main.table_products",
                ref_columns=["id"],
                name="fk_oi_product",
            ),
            IndexExists(
                "catalog_main", "catalog_main", "table_order_items", name="idx_oi_product", columns=["product_id"]
            ),
            TableExists("catalog_main", "catalog_main", "view_paid_orders"),
            TableKindIs("catalog_main", "catalog_main", "view_paid_orders", "view"),
            ColumnIs("catalog_main", "catalog_main", "view_paid_orders", "order_id", type="int"),
            ColumnIs("catalog_main", "catalog_main", "view_paid_orders", "user_id", type="int"),
            ColumnIs("catalog_main", "catalog_main", "view_paid_orders", "placed_at", type="timestamp"),
            TableExists("catalog_aux", "catalog_aux", "departments"),
            TableKindIs("catalog_aux", "catalog_aux", "departments", "table"),
            TableDescriptionContains("catalog_aux", "catalog_aux", "departments", "Departments"),
            PrimaryKeyIs("catalog_aux", "catalog_aux", "departments", ["id"]),
            UniqueConstraintExists("catalog_aux", "catalog_aux", "departments", ["name"], name="uq_departments_name"),
            TableExists("catalog_aux", "catalog_aux", "employees"),
            TableKindIs("catalog_aux", "catalog_aux", "employees", "table"),
            TableDescriptionContains("catalog_aux", "catalog_aux", "employees", "Employees"),
            ColumnIs(
                "catalog_aux",
                "catalog_aux",
                "employees",
                "active",
                type="tinyint(1)",
                nullable=False,
                default_equals="1",
            ),
            ColumnIs(
                "catalog_aux",
                "catalog_aux",
                "employees",
                "hired_at",
                type="timestamp",
                nullable=False,
                default_contains="CURRENT_TIMESTAMP",
            ),
            PrimaryKeyIs("catalog_aux", "catalog_aux", "employees", ["id"]),
            UniqueConstraintExists("catalog_aux", "catalog_aux", "employees", ["email"], name="uq_employees_email"),
            CheckConstraintExists("catalog_aux", "catalog_aux", "employees", name="chk_employees_salary"),
            ForeignKeyExists(
                "catalog_aux",
                "catalog_aux",
                "employees",
                from_columns=["department_id"],
                ref_table="catalog_aux.departments",
                ref_columns=["id"],
                name="fk_employees_department",
            ),
            IndexExists(
                "catalog_aux", "catalog_aux", "employees", name="idx_employees_dept", columns=["department_id"]
            ),
            TableExists("catalog_aux", "catalog_aux", "active_employees"),
            TableKindIs("catalog_aux", "catalog_aux", "active_employees", "view"),
            ColumnIs("catalog_aux", "catalog_aux", "active_employees", "id", type="int"),
            ColumnIs("catalog_aux", "catalog_aux", "active_employees", "department_id", type="int"),
            ColumnIs("catalog_aux", "catalog_aux", "active_employees", "email", type="varchar(255)"),
            ColumnIs("catalog_aux", "catalog_aux", "active_employees", "hired_at", type="timestamp"),
        ],
    )


def test_mysql_exact_samples(mysql_container_with_demo_schema, create_mysql_conn):
    rows = [
        {"id": 1, "sku": "SKU-1", "price": 10.50, "description": "foo"},
        {"id": 2, "sku": "SKU-2", "price": 20.00, "description": None},
    ]

    cleanup = [
        "DELETE FROM table_order_items;",
        "DELETE FROM table_products;",
    ]

    with seed_rows(create_mysql_conn, "catalog_main", "table_products", rows, cleanup_sql=cleanup):
        plugin = MySQLDbPlugin()
        config_file = _create_config_file_from_container(mysql_container_with_demo_schema)
        execution_result = execute_datasource_plugin(
            plugin, DatasourceType(full_type=config_file["type"]), config_file, "file_name"
        )
        assert isinstance(execution_result.result, DatabaseIntrospectionResult)

        assert_contract(
            execution_result.result,
            [
                SamplesEqual("catalog_main", "catalog_main", "table_products", rows=rows),
            ],
        )


def test_mysql_samples_in_big(mysql_container_with_demo_schema, create_mysql_conn):
    plugin = MySQLDbPlugin()
    limit = plugin._introspector._SAMPLE_LIMIT

    rows = [{"id": i, "sku": f"SKU-{i}", "price": float(i), "description": None} for i in range(1, 1000)]

    cleanup = [
        "DELETE FROM table_order_items;",
        "DELETE FROM table_products;",
    ]

    with seed_rows(create_mysql_conn, "catalog_main", "table_products", rows, cleanup_sql=cleanup):
        config_file = _create_config_file_from_container(mysql_container_with_demo_schema)
        execution_result = execute_datasource_plugin(
            plugin, DatasourceType(full_type=config_file["type"]), config_file, "file_name"
        )
        assert isinstance(execution_result.result, DatabaseIntrospectionResult)

        assert_contract(
            execution_result.result,
            [
                TableExists("catalog_main", "catalog_main", "table_products"),
                SamplesCountIs("catalog_main", "catalog_main", "table_products", count=limit),
            ],
        )


def _create_config_file_from_container(mysql: MySqlContainer) -> Mapping[str, Any]:
    return {
        "type": "databases/mysql",
        "connection": {
            "host": mysql.get_container_host_ip(),
            "port": int(mysql.get_exposed_port(mysql.port)),
            # TODO now this parameter is not used in introspections, worth checking if that is expected behaviour
            "database": mysql.dbname,
            "user": mysql.username,
            "password": mysql.password,
        },
    }
