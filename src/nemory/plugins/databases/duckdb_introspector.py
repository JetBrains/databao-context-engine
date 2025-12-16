from __future__ import annotations

from typing import Any

import duckdb
from pydantic import BaseModel, Field

from nemory.plugins.base_db_plugin import BaseDatabaseConfigFile
from nemory.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from nemory.plugins.databases.databases_types import (
    DatabaseColumn,
    DatasetKind,
    KeyConstraint,
    CheckConstraint,
    ForeignKey,
    ForeignKeyColumnMap,
    Index,
)


class DuckDBConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="databases/duckdb")
    connection: DuckDBConnectionConfig


class DuckDBConnectionConfig(BaseModel):
    database: str = Field(description="Path to the DuckDB database file")


class DuckDBIntrospector(BaseIntrospector[DuckDBConfigFile]):
    _IGNORED_CATALOGS = {"system", "temp"}
    _IGNORED_SCHEMAS = {"information_schema", "pg_catalog"}
    supports_catalogs = True

    def _connect(self, file_config: DuckDBConfigFile):
        database_path = str(file_config.connection.database)
        return duckdb.connect(database=database_path)

    def _get_catalogs(self, connection, file_config: DuckDBConfigFile) -> list[str]:
        res = connection.execute("SHOW DATABASES")
        rows = res.fetchall()
        return [r[0] for r in rows]

    def get_schemas(self, connection, catalog: str, file_config: DuckDBConfigFile) -> list[str]:
        res = connection.execute("SELECT schema_name FROM information_schema.schemata WHERE catalog_name = ? ORDER BY 1", [catalog])
        rows = res.fetchall()
        return [r[0] for r in rows]

    def collect_dataset_kinds(self, connection, catalog: str, schema: str) -> dict[str, DatasetKind]:
        res = connection.execute(
            """
            SELECT 
                table_name, 
                table_type
            FROM 
                information_schema.tables
            WHERE 
                table_schema = ?
            """,
            [schema],
        )
        kinds: dict[str, DatasetKind] = {}
        for table_name, table_type in res.fetchall():
            ttype = (table_type or "").upper()
            kinds[table_name] = DatasetKind.VIEW if ttype == "VIEW" else DatasetKind.TABLE
        return kinds

    def collect_columns(self, connection, catalog: str, schema: str) -> dict[str, list[DatabaseColumn]]:
        res = connection.execute(
            """
            SELECT
                table_name,
                column_name,
                data_type,
                is_nullable,
                column_default,
                ordinal_position
            FROM 
                information_schema.columns
            WHERE 
                table_schema = ?
            ORDER BY 
                table_name, 
                ordinal_position
            """,
            [schema],
        )
        columns_by_table: dict[str, list[DatabaseColumn]] = {}
        for table_name, column_name, data_type, is_nullable, column_default, _pos in res.fetchall():
            nullable = str(is_nullable).upper() == "YES"
            col = DatabaseColumn(
                name=column_name,
                type=data_type,
                nullable=nullable,
                description=None,
                default_expression=column_default,
                generated=None,
            )
            columns_by_table.setdefault(table_name, []).append(col)
        return columns_by_table

    def collect_primary_keys(self, connection, catalog: str, schema: str) -> dict[str, KeyConstraint | None]:
        res = connection.execute(
            """
            SELECT 
                table_name, 
                constraint_name, 
                constraint_column_names
            FROM 
                duckdb_constraints()
            WHERE 
                schema_name = ? 
              AND constraint_type = 'PRIMARY KEY'
            ORDER BY 
                table_name, 
                constraint_name
            """,
            [schema],
        )
        pks: dict[str, KeyConstraint | None] = {}
        for table_name, constraint_name, col_list in res.fetchall():
            cols = list(col_list or [])
            pks[table_name] = KeyConstraint(name=constraint_name, columns=cols, validated=None)
        return pks

    def collect_unique_constraints(self, connection, catalog: str, schema: str) -> dict[str, list[KeyConstraint]]:
        res = connection.execute(
            """
            SELECT 
                table_name, 
                constraint_name, 
                constraint_column_names
            FROM 
                duckdb_constraints()
            WHERE 
                schema_name = ? 
              AND constraint_type = 'UNIQUE'
            ORDER BY 
                table_name, 
                constraint_name
            """,
            [schema],
        )
        uniques: dict[str, list[KeyConstraint]] = {}
        for table_name, constraint_name, col_list in res.fetchall():
            cols = list(col_list or [])
            uniques.setdefault(table_name, []).append(
                KeyConstraint(name=constraint_name, columns=cols, validated=None)
            )
        return uniques

    def collect_table_checks(self, connection, catalog: str, schema: str) -> dict[str, list[CheckConstraint]]:
        res = connection.execute(
            """
            SELECT
              table_name,
              constraint_name,
              COALESCE(expression, constraint_text) AS expr
            FROM 
                duckdb_constraints()
            WHERE 
                schema_name = ? AND constraint_type = 'CHECK'
            ORDER BY 
                table_name, 
                constraint_name
            """,
            [schema],
        )
        checks: dict[str, list[CheckConstraint]] = {}
        for table_name, constraint_name, expr in res.fetchall():
            checks.setdefault(table_name, []).append(
                CheckConstraint(name=constraint_name, expression=str(expr), validated=None)
            )
        return checks

    def collect_foreign_keys(self, connection, catalog: str, schema: str) -> dict[str, list[ForeignKey]]:
        res = connection.execute(
            """
            SELECT
              fk.table_name,
              fk.constraint_name,
              fk.constraint_column_names    AS from_cols,
              COALESCE(rc.unique_constraint_schema, fk.schema_name) AS ref_schema,
              fk.referenced_table,
              fk.referenced_column_names    AS to_cols,
              rc.update_rule,
              rc.delete_rule
            FROM 
                duckdb_constraints() AS fk
                LEFT JOIN information_schema.referential_constraints rc ON rc.constraint_schema = fk.schema_name
                    AND rc.constraint_name   = fk.constraint_name
            WHERE 
                fk.schema_name = ?
                AND fk.constraint_type = 'FOREIGN KEY'
            ORDER BY 
                fk.table_name, 
                fk.constraint_name
            """,
            [schema],
        )

        by_tbl_con: dict[tuple[str, str], dict[str, Any]] = {}
        for table_name, conname, from_cols, ref_schema, ref_table, to_cols, upd_rule, del_rule in res.fetchall():
            key = (table_name, conname)
            entry = by_tbl_con.setdefault(
                key,
                {
                    "ref_schema": ref_schema,
                    "ref_table": ref_table,
                    "on_update": (upd_rule or "").lower() or None,
                    "on_delete": (del_rule or "").lower() or None,
                    "mapping": [],
                },
            )
            lhs = list(from_cols or [])
            rhs = list(to_cols or [])
            for f, t in zip(lhs, rhs):
                entry["mapping"].append((f, t))

        out: dict[str, list[ForeignKey]] = {}
        for (tbl, conname), info in by_tbl_con.items():
            mapping_objs = [ForeignKeyColumnMap(from_column=f, to_column=t) for f, t in info["mapping"]]
            fk = ForeignKey(
                name=conname,
                mapping=mapping_objs,
                referenced_table=self.qualify(catalog, info["ref_schema"], info["ref_table"]),
                enforced=True,
                validated=None,
                on_update=info["on_update"],
                on_delete=info["on_delete"],
            )
            out.setdefault(tbl, []).append(fk)
        return out

    def collect_indexes(self, connection, catalog: str, schema: str) -> dict[str, list[Index]]:
        """
        duckdb_indexes() lists secondary (CREATE INDEX) indexes.
        Column expressions are available in the 'expressions' column; cast to VARCHAR[].
        """
        res = connection.execute(
            """
            SELECT
                index_name,
                table_name,
                is_unique,
                expressions::VARCHAR[] AS exprs
            FROM 
                duckdb_indexes()
            WHERE 
                schema_name = ?
            ORDER BY 
                table_name, 
                index_name
            """,
            [schema],
        )

        indexes: dict[str, list[Index]] = {}
        for idx_name, table_name, is_unique, exprs in res.fetchall():
            cols = list(exprs or [])
            indexes.setdefault(table_name, []).append(
                Index(
                    name=idx_name,
                    columns=cols,
                    unique=bool(is_unique),
                    method=None,
                    predicate=None,
                )
            )
        return indexes