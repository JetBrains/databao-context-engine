import pytest

from databao_context_engine.plugins.databases.database_chunker import _build_column_chunk_text, _build_table_chunk_text
from databao_context_engine.plugins.databases.databases_types import (
    DatabaseColumn,
    DatabaseTable,
    ForeignKey,
    ForeignKeyColumnMap,
    KeyConstraint,
)


@pytest.fixture
def test_table():
    return DatabaseTable(
        name="my_table",
        columns=[
            DatabaseColumn(name="id", type="UUID", nullable=False),
            DatabaseColumn(name="account_id", type="UUID", nullable=False),
            DatabaseColumn(name="account_order_id", type="UUID", nullable=False),
            DatabaseColumn(name="account_order_date", type="DATETIME", nullable=False),
            DatabaseColumn(
                name="generated_column",
                type="VARCHAR",
                nullable=False,
                generated="computed",
                description="Computed value from other table information",
            ),
            DatabaseColumn(name="identity_column", type="INTEGER", nullable=True, generated="identity"),
        ],
        samples=[],
        partition_info=None,
        description=None,
        primary_key=KeyConstraint(name=None, columns=["id"], validated=None),
        foreign_keys=[
            ForeignKey(
                name=None,
                mapping=[ForeignKeyColumnMap(from_column="account_id", to_column="id")],
                referenced_table="account",
            ),
            ForeignKey(
                name=None,
                mapping=[
                    ForeignKeyColumnMap(from_column="account_order_id", to_column="id"),
                    ForeignKeyColumnMap(from_column="account_order_date", to_column="date"),
                ],
                referenced_table="account_order",
            ),
            ForeignKey(
                name=None,
                mapping=[ForeignKeyColumnMap(from_column="account_order_id", to_column="id")],
                referenced_table="order",
            ),
        ],
    )


def test_build_table_chunk_text(test_table):
    result = _build_table_chunk_text(test_table)

    assert (
        result
        == "my_table is a database table with 6 columns. Its primary key is the column id of type UUID. The table has foreign keys to account, account_order and order. Here is the full list of columns for the table: id, account_id, account_order_id, account_order_date, generated_column, identity_column"
    )


def test_build_column_chunk_text_with_primary_key(test_table):
    result = _build_column_chunk_text(test_table, next(column for column in test_table.columns if column.name == "id"))

    assert (
        result
        == "id is a column with type UUID in the table my_table. It can not contain null values. It is the primary key of the table"
    )


def test_build_column_chunk_text_with_foreign_key(test_table):
    result = _build_column_chunk_text(
        test_table, next(column for column in test_table.columns if column.name == "account_id")
    )

    assert (
        result
        == "account_id is a column with type UUID in the table my_table. It can not contain null values. This column is a foreign key to account.id"
    )


def test_build_column_chunk_text_part_of_one_foreign_key(test_table):
    result = _build_column_chunk_text(
        test_table, next(column for column in test_table.columns if column.name == "account_order_date")
    )

    assert (
        result
        == "account_order_date is a column with type DATETIME in the table my_table. It can not contain null values. This column is part of a foreign key to account_order"
    )


def test_build_column_chunk_text_part_of_multiple_foreign_key(test_table):
    result = _build_column_chunk_text(
        test_table, next(column for column in test_table.columns if column.name == "account_order_id")
    )

    assert (
        result
        == "account_order_id is a column with type UUID in the table my_table. It can not contain null values. This column is a foreign key to order.id. This column is part of a foreign key to account_order"
    )


def test_build_column_chunk_text_with_generated_column(test_table):
    result = _build_column_chunk_text(
        test_table, next(column for column in test_table.columns if column.name == "generated_column")
    )

    assert (
        result
        == "generated_column is a column with type VARCHAR in the table my_table. It can not contain null values. This column is a generated column. Computed value from other table information"
    )


def test_build_column_chunk_text_with_identity_column(test_table):
    result = _build_column_chunk_text(
        test_table, next(column for column in test_table.columns if column.name == "identity_column")
    )

    assert (
        result
        == "identity_column is a column with type INTEGER in the table my_table. It can contain null values. This column is an identity column"
    )
