from __future__ import annotations

import duckdb
from pydantic import BaseModel, Field

from databao_context_engine.plugins.base_db_plugin import BaseDatabaseConfigFile
from databao_context_engine.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from databao_context_engine.plugins.databases.databases_types import DatabaseSchema
from databao_context_engine.plugins.databases.introspection_model_builder import IntrospectionModelBuilder
from databao_context_engine.plugins.duckdb_tools import fetchall_dicts


class DuckDBConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="duckdb")
    connection: DuckDBConnectionConfig


class DuckDBConnectionConfig(BaseModel):
    database_path: str = Field(description="Path to the DuckDB database file")


class DuckDBIntrospector(BaseIntrospector[DuckDBConfigFile]):
    _IGNORED_CATALOGS = {"system", "temp"}
    _IGNORED_SCHEMAS = {"information_schema", "pg_catalog"}
    supports_catalogs = True

    def _connect(self, file_config: DuckDBConfigFile):
        database_path = str(file_config.connection.database_path)
        return duckdb.connect(database=database_path)

    def _connect_to_catalog(self, file_config: DuckDBConfigFile, catalog: str):
        return self._connect(file_config)

    def _get_catalogs(self, connection, file_config: DuckDBConfigFile) -> list[str]:
        rows = self._fetchall_dicts(connection, "SELECT database_name FROM duckdb_databases();", None)
        catalogs = [r["database_name"] for r in rows if r.get("database_name")]
        catalogs_filtered = [c for c in catalogs if c.lower() not in self._IGNORED_CATALOGS]
        return catalogs_filtered or [self._resolve_pseudo_catalog_name(file_config)]

    def _sql_list_schemas(self, catalogs: list[str] | None) -> SQLQuery:
        if not catalogs:
            return SQLQuery("SELECT schema_name, catalog_name FROM information_schema.schemata", None)
        sql = "SELECT catalog_name, schema_name FROM information_schema.schemata WHERE catalog_name = ANY(?)"
        return SQLQuery(sql, (catalogs,))

    def collect_catalog_model(self, connection, catalog: str, schemas: list[str]) -> list[DatabaseSchema] | None:
        if not schemas:
            return []

        comps = self._component_queries()
        results: dict[str, list[dict]] = {cq: [] for cq in comps}

        for cq, sql in comps.items():
            results[cq] = self._fetchall_dicts(connection, sql, (schemas,))

        return IntrospectionModelBuilder.build_schemas_from_components(
            schemas=schemas,
            rels=results.get("relations", []),
            cols=results.get("columns", []),
            pk_cols=results.get("pk", []),
            uq_cols=results.get("uq", []),
            checks=results.get("checks", []),
            fk_cols=results.get("fks", []),
            idx_cols=results.get("idx", []),
        )

    def _component_queries(self) -> dict[str, str]:
        return {
            "relations": self._sql_relations(),
            "columns": self._sql_columns(),
            "pk": self._sql_primary_keys(),
            "uq": self._sql_unique(),
            "checks": self._sql_checks(),
            "fks": self._sql_foreign_keys(),
            "idx": self._sql_indexes(),
        }

    def _sql_relations(self) -> str:
        return r"""
            SELECT
                table_schema AS schema_name,
                table_name,
                CASE table_type
                    WHEN 'BASE TABLE' THEN 'table'
                    WHEN 'VIEW' THEN 'view'
                    WHEN 'MATERIALIZED VIEW' THEN 'materialized_view'
                    ELSE lower(table_type)
                END AS kind,
                NULL::VARCHAR AS description
            FROM 
                information_schema.tables
            WHERE 
                table_schema = ANY(?)
            ORDER BY 
                table_name; 
        """

    def _sql_columns(self) -> str:
        return r"""
            SELECT
                c.table_schema AS schema_name,
                c.table_name,
                c.column_name,
                c.ordinal_position AS ordinal_position,
                c.data_type AS data_type,
                CASE 
                    WHEN c.is_nullable = 'YES' THEN TRUE 
                    ELSE FALSE 
                END AS is_nullable,
                c.column_default AS default_expression,
                NULL::VARCHAR AS generated,
                NULL::VARCHAR AS description
            FROM 
                information_schema.columns c
            WHERE 
                c.table_schema = ANY(?)
            ORDER BY 
                c.table_schema,
                c.table_name, 
                c.ordinal_position; 
        """

    def _sql_primary_keys(self) -> str:
        return r"""
            WITH d AS (
                SELECT 
                    *
                FROM 
                    duckdb_constraints()
                WHERE 
                    schema_name = ANY(?)
                    AND constraint_type = 'PRIMARY KEY'
            ),
            cols AS (
                SELECT
                    d.schema_name,
                    d.table_name,
                    d.constraint_name,
                    r.pos AS position,
                    d.constraint_column_names[r.pos] AS column_name
                FROM 
                    d,
                    range(1, length(d.constraint_column_names) + 1) AS r(pos)
            )
            SELECT
                schema_name,
                table_name,
                constraint_name,
                position,
                column_name
            FROM 
                cols
            ORDER BY
                schema_name,
                table_name, 
                constraint_name, 
                position;
        """

    def _sql_unique(self) -> str:
        return r"""
            WITH d AS (
                SELECT 
                    *
                FROM 
                    duckdb_constraints()
                WHERE 
                    schema_name = ANY(?)
                    AND constraint_type = 'UNIQUE'
            ),
            cols AS (
                SELECT
                    d.schema_name,
                    d.table_name,
                    d.constraint_name,
                    r.pos AS position,
                    d.constraint_column_names[r.pos] AS column_name
                FROM 
                    d,
                    range(1, length(d.constraint_column_names) + 1) AS r(pos)
            )
            SELECT
                schema_name,
                table_name,
                constraint_name,
                position,
                column_name
            FROM 
                cols
            ORDER BY
                schema_name,
                table_name, 
                constraint_name, 
                position;
        """

    def _sql_checks(self) -> str:
        return r"""
            SELECT
                d.schema_name,
                d.table_name,
                d.constraint_name,
                d.expression        AS expression,
                TRUE                AS validated
            FROM 
                duckdb_constraints() AS d
            WHERE 
                d.schema_name = ANY(?)
                AND d.constraint_type = 'CHECK'
            ORDER BY 
                d.schema_name, 
                d.table_name, 
                d.constraint_name; 
           """

    def _sql_foreign_keys(self) -> str:
        return r"""
            WITH d AS (
                SELECT 
                    *
                FROM 
                    duckdb_constraints()
                WHERE 
                    schema_name = ANY(?)
                    AND constraint_type = 'FOREIGN KEY'
            ),
            cols AS (
                SELECT
                    d.schema_name,
                    d.table_name,
                    d.constraint_name,
                    r.pos AS position,
                    d.constraint_column_names[r.pos]  AS from_column,
                    d.referenced_column_names[r.pos]  AS to_column
                FROM 
                    d,
                    range(1, length(d.constraint_column_names) + 1) AS r(pos)
            ),
            ref AS (
            SELECT
                rc.constraint_schema AS schema_name,
                rc.constraint_name,
                tc.table_schema AS ref_schema,
                tc.table_name   AS ref_table
            FROM 
                information_schema.referential_constraints rc
                JOIN information_schema.table_constraints tc ON 
                    tc.constraint_schema = rc.unique_constraint_schema 
                    AND tc.constraint_name = rc.unique_constraint_name
            ),
            rules AS (
                SELECT
                    constraint_schema AS schema_name,
                    constraint_name,
                    lower(update_rule) AS on_update,
                    lower(delete_rule) AS on_delete
                FROM 
                    information_schema.referential_constraints
            )
            SELECT
                c.schema_name,
                c.table_name,
                c.constraint_name,
                c.position,
                c.from_column,
                r.ref_schema,
                r.ref_table,
                c.to_column,
                coalesce(u.on_update, 'no action') AS on_update,
                coalesce(u.on_delete, 'no action') AS on_delete,
                TRUE AS enforced,
                TRUE AS validated
            FROM 
                cols c JOIN ref r ON r.schema_name = c.schema_name AND r.constraint_name = c.constraint_name
            LEFT JOIN rules u ON u.schema_name = c.schema_name AND u.constraint_name = c.constraint_name
            ORDER BY 
                c.schema_name, 
                c.table_name, 
                c.constraint_name, 
                c.position;
        """

    def _sql_indexes(self) -> str:
        return r"""
            WITH idx AS (
                SELECT
                    schema_name,
                    table_name,
                    index_name,
                    is_unique,
                    string_split(trim(BOTH '[]' FROM expressions), ',') AS expr_list
                FROM 
                    duckdb_indexes()
                WHERE 
                    schema_name = ANY(?)
            )
            SELECT
                schema_name,
                table_name,
                index_name,
                pos AS position,
                trim(expr_list[pos]) AS expr,
                is_unique
            FROM 
                idx,
                range(1, length(expr_list) + 1) AS r(pos)
            ORDER BY
                schema_name, 
                table_name,
                index_name,
                position;
         """

    def _sql_sample_rows(self, catalog: str, schema: str, table: str, limit: int) -> SQLQuery:
        sql = f'SELECT * FROM "{schema}"."{table}" LIMIT ?'
        return SQLQuery(sql, (limit,))

    def _quote_literal(self, value: str) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        cur = connection.cursor()
        return fetchall_dicts(cur, sql, params)
