import sqlite3
from pathlib import Path

from typing_extensions import override

from databao_context_engine.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from databao_context_engine.plugins.databases.sqlite.config_file import SQLiteConfigFile


class SQLiteIntrospector(BaseIntrospector[SQLiteConfigFile]):
    _IGNORED_SCHEMAS = {"temp", "information_schema"}
    _PSEUDO_SCHEMA = "main"
    supports_catalogs = False

    def _connect(self, file_config: SQLiteConfigFile, *, catalog: str | None = None):
        database_path = Path(file_config.connection.database_path)
        if not database_path.is_file():
            raise ConnectionError(f"No SQLite database was found at path {database_path.resolve()}")

        conn = sqlite3.connect(database_path)
        conn.text_factory = str
        return conn

    def _connection_check_sql_query(self) -> str:
        return "SELECT name FROM sqlite_master LIMIT 1"

    def _get_catalogs(self, connection, file_config: SQLiteConfigFile) -> list[str]:
        return [self._resolve_pseudo_catalog_name(file_config)]

    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        cur = connection.cursor()
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)

        if cur.description is None:
            return []

        rows = cur.fetchall()
        out: list[dict] = []
        for r in rows:
            if isinstance(r, sqlite3.Row):
                out.append({k.lower(): r[k] for k in r.keys()})
            else:
                cols = [d[0].lower() for d in cur.description]
                out.append(dict(zip(cols, r)))
        return out

    def _list_schemas_for_catalog(self, connection, catalog: str) -> list[str]:
        return [self._PSEUDO_SCHEMA]

    @override
    def get_relations_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        return SQLQuery(
            sql=f"""
            SELECT
                '{self._PSEUDO_SCHEMA}' AS schema_name,
                m.name AS table_name,
                CASE m.type
                    WHEN 'view' THEN 'view'
                    ELSE 'table'
                END AS kind,
                NULL AS description
            FROM 
                sqlite_master m
            WHERE
                m.type IN ('table', 'view')
                AND m.name NOT LIKE 'sqlite_%'
            ORDER BY
                m.name;
        """
        )

    @override
    def get_table_columns_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        return self._columns_sql_query("m.type = 'table'")

    @override
    def get_view_columns_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        return self._columns_sql_query("m.type = 'view'")

    def _columns_sql_query(self, table_type_filter: str) -> SQLQuery:
        return SQLQuery(
            sql=f"""
            SELECT
                '{self._PSEUDO_SCHEMA}' AS schema_name,
                m.name AS table_name,
                c.name AS column_name,
                (c.cid + 1) AS ordinal_position,
                COALESCE(c.type,'') AS data_type,
                CASE 
                    WHEN c.pk > 0 THEN 0 
                    WHEN c."notnull" = 0 THEN 1 
                    ELSE 0 
                END AS is_nullable,
                c.dflt_value AS default_expression,
                CASE
                    WHEN c.hidden IN (2,3) THEN 'computed'
                END AS generated,
                NULL AS description
            FROM 
                sqlite_master m
                JOIN pragma_table_xinfo(m.name) c
            WHERE 
                {table_type_filter}
                AND m.name NOT LIKE 'sqlite_%'
            ORDER BY 
                m.name, 
                c.cid;
        """
        )

    @override
    def get_primary_keys_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        return SQLQuery(
            sql=f"""
            SELECT
                '{self._PSEUDO_SCHEMA}' AS schema_name,
                m.name AS table_name,
                ('pk_' || m.name) AS constraint_name,
                c.pk AS position,
                c.name AS column_name
            FROM 
                sqlite_master m
                JOIN pragma_table_info(m.name) c
            WHERE
                m.type = 'table'
                AND m.name NOT LIKE 'sqlite_%'
                AND c.pk > 0
            ORDER BY
                m.name,
                c.pk;
        """
        )

    @override
    def get_unique_constraints_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        return SQLQuery(
            sql=f"""
            SELECT
                '{self._PSEUDO_SCHEMA}' AS schema_name,
                m.name AS table_name,
                il.name AS constraint_name,
                (ii.seqno + 1) AS position,
                ii.name AS column_name
            FROM 
                sqlite_master m
                JOIN pragma_index_list(m.name) il
                JOIN pragma_index_info(il.name) ii
            WHERE
                m.type = 'table'
                AND m.name NOT LIKE 'sqlite_%'
                AND il."unique" = 1
                AND il.origin = 'u'
            ORDER BY
                m.name,
                il.name,
                ii.seqno;
        """
        )

    @override
    def get_foreign_keys_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        return SQLQuery(
            sql=f"""
            SELECT
                '{self._PSEUDO_SCHEMA}' AS schema_name,
                m.name AS table_name,
                ('fk_' || m.name || '_' || fk.id) AS constraint_name,
                (fk.seq + 1) AS position,
                fk."from" AS from_column,
                'main' AS ref_schema,
                fk."table" AS ref_table,
                fk."to" AS to_column,
                lower(fk.on_update) AS on_update,
                lower(fk.on_delete) AS on_delete,
                1 AS enforced,
                1 AS validated
            FROM sqlite_master m
            JOIN pragma_foreign_key_list(m.name) fk
            WHERE
                m.type = 'table'
                AND m.name NOT LIKE 'sqlite_%'
            ORDER BY
                m.name,
                fk.id,
                fk.seq;
        """
        )

    @override
    def get_indexes_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        return SQLQuery(
            sql=f"""
            SELECT
                '{self._PSEUDO_SCHEMA}' AS schema_name,
                m.name AS table_name,
                il.name AS index_name,
                (ix.seqno + 1) AS position,
                ix.name AS expr,
                il."unique" AS is_unique,
                NULL AS method,
                CASE
                    WHEN il.partial = 1 AND sm.sql IS NOT NULL AND instr(upper(sm.sql), 'WHERE') > 0
                    THEN trim(substr(sm.sql, instr(upper(sm.sql), 'WHERE') + length('WHERE')))
                END AS predicate
            FROM 
                sqlite_master m
                JOIN pragma_index_list(m.name) il
                JOIN pragma_index_xinfo(il.name) ix
                LEFT JOIN sqlite_master sm ON sm.type = 'index' AND sm.name = il.name
            WHERE 
                m.type='table'
                AND m.name NOT LIKE 'sqlite_%'
                AND lower(il.origin) = 'c'
                AND ix.key = 1
            ORDER BY 
                m.name, 
                il.name, 
                ix.seqno;
        """
        )

    def _sql_sample_rows(self, catalog: str, schema: str, table: str, limit: int) -> SQLQuery:
        sql = f"SELECT * FROM {self._quote_ident(table)} LIMIT ?"
        return SQLQuery(sql, (limit,))

    def _quote_ident(self, ident: str) -> str:
        return '"' + str(ident).replace('"', '""') + '"'
