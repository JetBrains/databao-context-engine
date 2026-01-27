import sqlite3

from pydantic import BaseModel, Field

from databao_context_engine.plugins.base_db_plugin import BaseDatabaseConfigFile
from databao_context_engine.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from databao_context_engine.plugins.databases.databases_types import DatabaseSchema
from databao_context_engine.plugins.databases.introspection_model_builder import IntrospectionModelBuilder


class SQLiteConnectionConfig(BaseModel):
    database_path: str = Field(description="Path to the SQLite database file")


class SQLiteConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="databases/sqlite")
    connection: SQLiteConnectionConfig


class SQLiteIntrospector(BaseIntrospector[SQLiteConfigFile]):
    _IGNORED_SCHEMAS = {"temp", "information_schema"}
    _PSEUDO_SCHEMA = "main"
    supports_catalogs = False

    def _connect(self, file_config: SQLiteConfigFile):
        database_path = str(file_config.connection.database_path)
        conn = sqlite3.connect(database_path)
        conn.text_factory = str
        return conn

    def _connect_to_catalog(self, file_config: SQLiteConfigFile, catalog: str):
        return self._connect(file_config)

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

    def collect_catalog_model(self, connection, catalog: str, schemas: list[str]) -> list[DatabaseSchema] | None:
        if not schemas:
            return []

        comps = self._component_queries()
        results: dict[str, list[dict]] = {name: [] for name in comps}

        for name, sql in comps.items():
            results[name] = self._fetchall_dicts(connection, sql, None) or []

        return IntrospectionModelBuilder.build_schemas_from_components(
            schemas=[self._PSEUDO_SCHEMA],
            rels=results.get("relations", []),
            cols=results.get("columns", []),
            pk_cols=results.get("pk", []),
            uq_cols=results.get("uq", []),
            checks=[],
            fk_cols=results.get("fks", []),
            idx_cols=results.get("idx", []),
            partitions=[],
        )

    def _component_queries(self) -> dict[str, str]:
        return {
            "relations": self._sql_relations(),
            "columns": self._sql_columns(),
            "pk": self._sql_primary_keys(),
            "uq": self._sql_unique(),
            "fks": self._sql_foreign_keys(),
            "idx": self._sql_indexes(),
        }

    def _sql_relations(self) -> str:
        return f"""
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

    def _sql_columns(self) -> str:
        return f"""
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
                m.type IN ('table','view')
                AND m.name NOT LIKE 'sqlite_%'
            ORDER BY 
                m.name, 
                c.cid;
        """

    def _sql_primary_keys(self) -> str:
        return f"""
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

    def _sql_unique(self) -> str:
        return f"""
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

    def _sql_foreign_keys(self) -> str:
        return f"""
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

    def _sql_indexes(self) -> str:
        return f"""
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

    def _sql_sample_rows(self, catalog: str, schema: str, table: str, limit: int) -> SQLQuery:
        sql = f"SELECT * FROM {self._quote_ident(table)} LIMIT ?"
        return SQLQuery(sql, (limit,))

    def _quote_ident(self, ident: str) -> str:
        return '"' + str(ident).replace('"', '""') + '"'
