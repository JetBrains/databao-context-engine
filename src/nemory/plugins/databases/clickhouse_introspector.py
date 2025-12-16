from __future__ import annotations

import re
from io import UnsupportedOperation
from typing import Any, Mapping

import clickhouse_connect
from pydantic import Field

from nemory.plugins.base_db_plugin import BaseDatabaseConfigFile
from nemory.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from nemory.plugins.databases.databases_types import (
    DatabaseColumn,
    DatasetKind,
    CheckConstraint,
    ForeignKey,
    Index,
    KeyConstraint,
)


class ClickhouseConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="databases/clickhouse")
    connection: dict[str, Any] = Field(
        description="Connection parameters for the Clickhouse database. It can contain any of the keys supported by the Clickhouse connection library (see https://clickhouse.com/docs/integrations/language-clients/python/driver-api#connection-arguments)"
    )


class ClickhouseIntrospector(BaseIntrospector[ClickhouseConfigFile]):
    _IGNORED_SCHEMAS = {"information_schema", "system"}

    supports_catalogs = False

    def _connect(self, file_config: ClickhouseConfigFile):
        connection = file_config.connection
        if not isinstance(connection, Mapping):
            raise ValueError("Invalid YAML config: 'connection' must be a mapping of connection parameters")

        return clickhouse_connect.get_client(**connection)

    def _get_catalogs(self, connection, file_config: ClickhouseConfigFile) -> list[str]:
        raise UnsupportedOperation("Clickhouse doesnt support catalogs")

    def get_schemas(self, connection, catalog: str, file_config: ClickhouseConfigFile) -> list[str]:
        """
        Return ClickHouse databases. The base will apply _IGNORED_SCHEMAS filtering.
        """
        rows = self._query_dicts(connection, "SELECT name FROM system.databases ORDER BY name")
        return [r["name"] for r in rows]

    def collect_dataset_kinds(self, connection, catalog: str, schema: str) -> dict[str, DatasetKind]:
        """
        Map table name -> kind based on the table engine.
        """
        rows = self._query_dicts(
            connection,
            """
            SELECT 
                name AS table_name, 
                engine
            FROM 
                system.tables
            WHERE 
                database = %(db)s
            """,
            {"db": schema},
        )

        external_engines = {"File", "S3", "HDFS", "URL", "Kafka", "MySQL", "JDBC", "ODBC", "PostgreSQL", "MongoDB"}

        kinds: dict[str, DatasetKind] = {}
        for r in rows:
            eng = (r["engine"] or "").strip()
            if eng == "View" or eng == "LiveView":
                kinds[r["table_name"]] = DatasetKind.VIEW
            elif eng == "MaterializedView":
                kinds[r["table_name"]] = DatasetKind.MATERIALIZED_VIEW
            elif eng in external_engines:
                kinds[r["table_name"]] = DatasetKind.EXTERNAL_TABLE
            else:
                kinds[r["table_name"]] = DatasetKind.TABLE
        return kinds

    def collect_columns(self, connection, catalog: str, schema: str) -> dict[str, list[DatabaseColumn]]:
        """
        Source: system.columns
        - type: ClickHouse type string, with Nullable(...) unwrapped
        - nullable: inferred from Nullable(...)
        - default_expression: from default_expression (empty -> None)
        - generated: 'MATERIALIZED'/'ALIAS' => 'computed'; 'DEFAULT' => None
        - description: column comment (if available)
        """
        rows = self._query_dicts(
            connection,
            """
            SELECT
                table          AS table_name,
                position       AS position,
                name           AS column_name,
                type           AS type_str,
                default_kind   AS default_kind,
                default_expression,
                comment        AS column_comment
            FROM 
                system.columns
            WHERE 
                database = %(db)s
            ORDER BY 
                table, 
                position
            """,
            {"db": schema},
        )

        by_table: dict[str, list[DatabaseColumn]] = {}
        for r in rows:
            base_type, is_nullable = self._clean_ch_type(r["type_str"] or "")
            default_kind = (r.get("default_kind") or "").upper()
            default_expr = r.get("default_expression") or None

            generated: str | None
            if default_kind in ("MATERIALIZED", "ALIAS"):
                generated = "computed"
            else:
                generated = None

            col = DatabaseColumn(
                name=r["column_name"],
                type=base_type,
                nullable=is_nullable,
                description=r.get("column_comment") or None,
                default_expression=default_expr,
                generated=generated,
            )

            by_table.setdefault(r["table_name"], []).append(col)

        return by_table

    def collect_primary_keys(self, connection, catalog: str, schema: str) -> dict[str, KeyConstraint | None]:
        """
        ClickHouse doesn't have relational primary keys (its PRIMARY KEY is a data-skip index).
        Return {} and let the base default to None.
        """
        return {}

    def collect_unique_constraints(self, connection, catalog: str, schema: str) -> dict[str, list[KeyConstraint]]:
        """
        ClickHouse doesn't enforce uniqueness constraints.
        """
        return {}

    def collect_table_checks(self, connection, catalog: str, schema: str) -> dict[str, list[CheckConstraint]]:
        """
        Parse CONSTRAINT ... CHECK (...) from create_table_query.
        There is no dedicated system table for constraints today.
        """
        # TODO: implement this. Possible, but difficult (requires regex parsing)
        return {}

    def collect_foreign_keys(self, connection, catalog: str, schema: str) -> dict[str, list[ForeignKey]]:
        """
        ClickHouse has no foreign keys; return {}.
        """
        return {}

    def collect_indexes(self, connection, catalog: str, schema: str) -> dict[str, list[Index]]:
        """
        Source: system.data_skipping_indices (ClickHouse 21+)
        Fallback: parse from create_table_query if the system table is missing.
        """
        indexes: dict[str, list[Index]] = {}

        rows = self._query_dicts(
            connection,
            """
            SELECT
                table       AS table_name,
                name        AS index_name,
                type        AS method,
                expr,
                granularity
            FROM system.data_skipping_indices
            WHERE database = %(db)s
            ORDER BY table, index_name
            """,
            {"db": schema},
        )
        for r in rows:
            idx = Index(
                name=r["index_name"],
                columns=[(r.get("expr") or "").strip()],
                unique=False,
                method=(r.get("method") or "").lower() or None,
                predicate=None,
            )
            indexes.setdefault(r["table_name"], []).append(idx)
        return indexes

    def _query_dicts(self, connection, sql: str, params: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        result = connection.query(sql, parameters=params) if params else connection.query(sql)
        cols = result.column_names
        return [dict(zip(cols, row)) for row in result.result_rows]

    @staticmethod
    def _clean_ch_type(t: str) -> tuple[str, bool]:
        """
        Normalize a ClickHouse type string:
        - unwrap Nullable(T) -> (T, True)
        - otherwise -> (t, False)
        """
        if t.startswith("Nullable(") and t.endswith(")"):
            return t[len("Nullable(") : -1], True
        return t, False
