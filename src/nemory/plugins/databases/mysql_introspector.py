from typing import Any, Mapping

import pymysql
from pydantic import Field
from pymysql.constants import CLIENT

from nemory.plugins.base_db_plugin import BaseDatabaseConfigFile
from nemory.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from nemory.plugins.databases.databases_types import DatabaseTable
from nemory.plugins.databases.table_builder import TableBuilder


class MySQLConfigFile(BaseDatabaseConfigFile):
    connection: dict[str, Any]
    type: str = Field(default="databases/mysql")


class MySQLIntrospector(BaseIntrospector[MySQLConfigFile]):
    _IGNORED_SCHEMAS = {"information_schema", "mysql", "performance_schema", "sys"}

    supports_catalogs = True

    def _connect(self, file_config: MySQLConfigFile):
        connection = file_config.connection
        if not isinstance(connection, Mapping):
            raise ValueError("Invalid YAML config: 'connection' must be a mapping of connection parameters")

        return pymysql.connect(
            **connection,
            cursorclass=pymysql.cursors.DictCursor,
            client_flag=CLIENT.MULTI_STATEMENTS | CLIENT.MULTI_RESULTS,
        )

    def _connect_to_catalog(self, file_config: MySQLConfigFile, catalog: str):
        cfg = dict(file_config.connection or {})
        cfg["database"] = catalog
        return self._connect(MySQLConfigFile(connection=cfg))

    def _get_catalogs(self, connection, file_config: MySQLConfigFile) -> list[str]:
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT schema_name
                FROM information_schema.schemata
                ORDER BY schema_name
                """
            )
            dbs = [row["SCHEMA_NAME"] for row in cur.fetchall()]
        return [d for d in dbs if d.lower() not in self._IGNORED_SCHEMAS]

    def _sql_list_schemas(self, catalogs: list[str] | None) -> SQLQuery:
        return SQLQuery(
            "SELECT DATABASE() AS schema_name, DATABASE() AS catalog_name",
            None,
        )

    def collect_schema_model(self, connection, catalog: str, schema: str) -> list[DatabaseTable] | None:
        comps = self._component_queries(schema)
        results: dict[str, list[dict]] = {cq["name"]: [] for cq in comps}

        batch = (
            ";\n".join(cq["sql"].replace("{SCHEMA}", self._quote_literal(schema)).rstrip().rstrip(";") for cq in comps)
            + ";"
        )

        with connection.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(batch)

            for ix, cq in enumerate(comps, start=1):
                name = cq["name"]

                raw_rows = cur.fetchall() if cur.description else ()

                rows_list: list[dict]
                # TODO: simplify this
                if raw_rows and isinstance(raw_rows[0], dict):
                    rows_list = [{k.lower(): v for k, v in row.items()} for row in raw_rows]
                else:
                    if cur.description:
                        cols = [d[0].lower() for d in cur.description]
                        rows_list = [dict(zip(cols, r)) for r in raw_rows]
                    else:
                        rows_list = []

                results[name] = rows_list

                if ix < len(comps):
                    ok = cur.nextset()
                    if not ok:
                        raise RuntimeError(f"MySQL batch ended early after component #{ix} '{name}'")

        return TableBuilder.build_from_components(
            rels=results.get("relations", []),
            cols=results.get("columns", []),
            pk_cols=results.get("pk", []),
            uq_cols=results.get("uq", []),
            checks=results.get("checks", []),
            fk_cols=results.get("fks", []),
            idx_cols=results.get("idx", []),
        )

    def _component_queries(self, schema: str) -> list[dict]:
        return [
            {"name": "relations", "sql": self._sql_relations()},
            {"name": "columns", "sql": self._sql_columns()},
            {"name": "pk", "sql": self._sql_primary_keys()},
            {"name": "uq", "sql": self._sql_uniques()},
            {"name": "checks", "sql": self._sql_checks()},
            {"name": "fks", "sql": self._sql_foreign_keys()},
            {"name": "idx", "sql": self._sql_indexes()},
        ]

    def _sql_relations(self) -> str:
        return r"""
            SELECT
                t.TABLE_NAME        AS table_name,
                CASE t.TABLE_TYPE
                    WHEN 'BASE TABLE'  THEN 'table'
                    WHEN 'VIEW'        THEN 'view'
                    ELSE LOWER(t.TABLE_TYPE)
                END                 AS kind,
                CASE t.TABLE_TYPE
                    WHEN 'VIEW' THEN NULL
                    ELSE NULLIF(t.TABLE_COMMENT, '')
                END                 AS description
            FROM 
                INFORMATION_SCHEMA.TABLES t
            WHERE 
                t.TABLE_SCHEMA = {SCHEMA}
            ORDER BY 
                t.TABLE_NAME
        """

    def _sql_columns(self) -> str:
        return r"""
            SELECT
                c.TABLE_NAME                         AS table_name,
                c.COLUMN_NAME                        AS column_name,
                c.ORDINAL_POSITION                   AS ordinal_position,
                c.COLUMN_TYPE                        AS data_type,
                CASE 
                    WHEN c.IS_NULLABLE = 'YES' THEN TRUE 
                    ELSE FALSE 
                END AS is_nullable,
                CASE
                    WHEN c.EXTRA RLIKE '\\b(VIRTUAL|STORED) GENERATED\\b' THEN NULLIF(c.GENERATION_EXPRESSION, '')
                    ELSE c.COLUMN_DEFAULT
                END AS default_expression,
                CASE
                    WHEN c.EXTRA LIKE '%auto_increment%' THEN 'identity'
                    WHEN c.EXTRA RLIKE '\\b(VIRTUAL|STORED) GENERATED\\b' THEN 'computed'
                    ELSE NULL
                END AS "generated",
                NULLIF(c.COLUMN_COMMENT, '')         AS description
            FROM 
                INFORMATION_SCHEMA.COLUMNS c
            WHERE 
                c.TABLE_SCHEMA = {SCHEMA}
            ORDER BY 
                c.TABLE_NAME, 
                c.ORDINAL_POSITION
        """

    def _sql_primary_keys(self) -> str:
        return r"""
            SELECT
                tc.TABLE_NAME         AS table_name,
                tc.CONSTRAINT_NAME    AS constraint_name,
                kcu.COLUMN_NAME       AS column_name,
                kcu.ORDINAL_POSITION  AS position
            FROM 
                INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
                     ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA AND kcu.TABLE_NAME = tc.TABLE_NAME
            WHERE 
                tc.TABLE_SCHEMA = {SCHEMA}
                AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY 
                tc.TABLE_NAME, 
                tc.CONSTRAINT_NAME, 
                kcu.ORDINAL_POSITION
        """

    def _sql_uniques(self) -> str:
        return r"""
            SELECT
                tc.TABLE_NAME         AS table_name,
                tc.CONSTRAINT_NAME    AS constraint_name,
                kcu.COLUMN_NAME       AS column_name,
                kcu.ORDINAL_POSITION  AS position
            FROM 
                INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA AND kcu.TABLE_NAME = tc.TABLE_NAME
            WHERE 
                tc.TABLE_SCHEMA = {SCHEMA}
                AND tc.CONSTRAINT_TYPE = 'UNIQUE'
            ORDER BY 
                tc.TABLE_NAME, 
                tc.CONSTRAINT_NAME, 
                kcu.ORDINAL_POSITION
        """

    def _sql_checks(self) -> str:
        return r"""
            SELECT
                tc.TABLE_NAME        AS table_name,
                tc.CONSTRAINT_NAME   AS constraint_name,
                cc.CHECK_CLAUSE      AS expression,
                TRUE                 AS validated
            FROM 
                INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc ON cc.CONSTRAINT_SCHEMA = tc.TABLE_SCHEMA AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            WHERE 
                tc.TABLE_SCHEMA = {SCHEMA}
                AND tc.CONSTRAINT_TYPE = 'CHECK'
            ORDER BY 
                tc.TABLE_NAME, 
                tc.CONSTRAINT_NAME
        """

    def _sql_foreign_keys(self) -> str:
        return r"""
            SELECT
                kcu.TABLE_NAME                 AS table_name,
                kcu.CONSTRAINT_NAME            AS constraint_name,
                kcu.ORDINAL_POSITION           AS position,
                kcu.COLUMN_NAME                AS from_column,
                kcu.REFERENCED_TABLE_SCHEMA    AS ref_schema,
                kcu.REFERENCED_TABLE_NAME      AS ref_table,
                kcu.REFERENCED_COLUMN_NAME     AS to_column,
                LOWER(rc.UPDATE_RULE)          AS on_update,
                LOWER(rc.DELETE_RULE)          AS on_delete,
                TRUE                           AS enforced,
                TRUE                           AS validated
            FROM 
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA AND tc.TABLE_NAME = kcu.TABLE_NAME
                JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc ON rc.CONSTRAINT_SCHEMA = kcu.TABLE_SCHEMA AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            WHERE 
                kcu.TABLE_SCHEMA = {SCHEMA}
                AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
            ORDER BY 
                kcu.TABLE_NAME, 
                kcu.CONSTRAINT_NAME, 
                kcu.ORDINAL_POSITION
        """

    def _sql_indexes(self) -> str:
        return r"""
            SELECT
                s.TABLE_NAME                                    AS table_name,
                s.INDEX_NAME                                    AS index_name,
                s.SEQ_IN_INDEX                                  AS position,
                COALESCE(s.EXPRESSION, s.COLUMN_NAME)           AS expr,
                (s.NON_UNIQUE = 0)                              AS is_unique,
                s.INDEX_TYPE                                    AS method,
                NULL                                            AS predicate
            FROM 
                INFORMATION_SCHEMA.STATISTICS s
            WHERE 
                s.TABLE_SCHEMA = {SCHEMA}
                AND s.INDEX_NAME <> 'PRIMARY'
            ORDER BY 
                s.TABLE_NAME, 
                s.INDEX_NAME, 
                s.SEQ_IN_INDEX
        """

    def _sql_sample_rows(self, catalog: str, schema: str, table: str, limit: int) -> SQLQuery:
        sql = f"SELECT * FROM `{schema}`.`{table}` LIMIT %s"
        return SQLQuery(sql, (limit,))

    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        with connection.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [{k.lower(): v for k, v in row.items()} for row in rows]

    def _quote_literal(self, value: str) -> str:
        return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"

    def _quote_ident(self, ident: str) -> str:
        return "`" + ident.replace("`", "``") + "`"
