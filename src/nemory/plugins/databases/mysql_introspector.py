from typing import Any, Mapping

import pymysql
from pydantic import Field

from nemory.plugins.base_db_plugin import BaseDatabaseConfigFile
from nemory.plugins.databases.base_introspector import BaseIntrospector
from nemory.plugins.databases.databases_types import (
    CheckConstraint,
    DatabaseColumn,
    DatasetKind,
    ForeignKey,
    ForeignKeyColumnMap,
    Index,
    KeyConstraint,
)


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
        )

    def _get_catalogs(self, connection, file_config: MySQLConfigFile) -> list[str]:
        """
        Enumerate MySQL databases as catalogs, excluding system ones.
        """
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

    def config_for_catalog(self, file_config: MySQLConfigFile, catalog: str) -> MySQLConfigFile:
        """
        Reconnect per catalog by setting 'database' (and 'db' for compatibility) in the config.
        """
        try:
            cfg = file_config.model_copy(deep=True)  # pydantic v2
        except AttributeError:
            cfg = file_config.copy(deep=True)        # pydantic v1
        conn = dict(cfg.connection)
        conn["database"] = catalog
        conn["db"] = catalog  # some clients alias this; harmless to set both
        cfg.connection = conn
        return cfg

    def get_schemas(self, connection, catalog: str, file_config: MySQLConfigFile) -> list[str]:
        """
        MySQL doesn't have schemas inside a database; we treat the database as the only schema.
        """
        return [catalog]

    def collect_dataset_kinds(self, connection, catalog: str, schema: str) -> dict[str, DatasetKind]:
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT 
                    table_name, 
                    table_type
                FROM 
                    information_schema.tables
                WHERE 
                    table_schema = %s
                """,
                (schema,),
            )
            kinds: dict[str, DatasetKind] = {}
            for row in cur.fetchall():
                ttype = (row["TABLE_TYPE"] or "").upper()
                kinds[row["TABLE_NAME"]] = DatasetKind.VIEW if ttype == "VIEW" else DatasetKind.TABLE
            return kinds

    def collect_columns(self, connection, catalog: str, schema: str) -> dict[str, list[DatabaseColumn]]:
        """
        Pushes generation/default/view logic to SQL for a simpler Python loop:
          - IS_GENERATED: computed in SQL from GENERATION_EXPRESSION / EXTRA
          - DEFAULT_EXPR: generation_expression if generated; NULL for views; else column_default
        """
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT
                    t.table_name,
                    t.table_type,
                    c.column_name,
                    c.data_type,
                    c.is_nullable,
                    NULLIF(c.column_comment,'') AS COLUMN_COMMENT,
                    c.ordinal_position,
CASE
  WHEN UPPER(t.table_type) = 'VIEW' THEN 0
  WHEN c.generation_expression IS NOT NULL AND c.generation_expression <> '' THEN 1
  WHEN UPPER(c.extra) RLIKE '(^| )VIRTUAL GENERATED( |$)|(^| )STORED GENERATED( |$)' THEN 1
  ELSE 0
END AS IS_GENERATED,
CASE
  WHEN (c.generation_expression IS NOT NULL AND c.generation_expression <> '')
    OR UPPER(c.extra) RLIKE '(^| )VIRTUAL GENERATED( |$)|(^| )STORED GENERATED( |$)'
    THEN c.generation_expression
  WHEN UPPER(t.table_type) = 'VIEW' THEN NULL
  ELSE c.column_default
END AS DEFAULT_EXPR
                FROM information_schema.columns c
                         JOIN information_schema.tables  t
                              ON t.table_schema = c.table_schema
                                  AND t.table_name   = c.table_name
                WHERE c.table_schema = %s
                ORDER BY t.table_name, c.ordinal_position
                """,
                (schema,),
            )
            by_table: dict[str, list[DatabaseColumn]] = {}
            for r in cur.fetchall():
                nullable = str(r["IS_NULLABLE"]).upper() == "YES"
                is_generated = bool(r["IS_GENERATED"])
                default_expr = r["DEFAULT_EXPR"]

                col = DatabaseColumn(
                    name=r["COLUMN_NAME"],
                    type=r["DATA_TYPE"],
                    nullable=nullable,
                    description=(r.get("COLUMN_COMMENT") or None),
                    default_expression=default_expr,
                    generated="computed" if is_generated else None,
                )
                by_table.setdefault(r["TABLE_NAME"], []).append(col)
        return by_table

    def collect_primary_keys(self, connection, catalog: str, schema: str) -> dict[str, KeyConstraint | None]:
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT
                    tc.table_name,
                    tc.constraint_name,
                    k.ordinal_position,
                    k.column_name
                FROM information_schema.table_constraints AS tc
                         JOIN information_schema.key_column_usage AS k
                              ON k.constraint_schema = tc.constraint_schema
                                  AND k.constraint_name  = tc.constraint_name
                                  AND k.table_schema     = tc.table_schema
                                  AND k.table_name       = tc.table_name
                WHERE tc.table_schema = %s
                  AND tc.constraint_type = 'PRIMARY KEY'
                ORDER BY tc.table_name, tc.constraint_name, k.ordinal_position
                """,
                (schema,),
            )
            by_tbl: dict[str, dict[str, Any]] = {}
            for r in cur.fetchall():
                entry = by_tbl.setdefault(r["TABLE_NAME"], {"name": r["CONSTRAINT_NAME"], "cols": []})
                entry["cols"].append(r["COLUMN_NAME"])
        return {
            tbl: KeyConstraint(name=info["name"], columns=info["cols"], validated=None) for tbl, info in by_tbl.items()
        }

    def collect_unique_constraints(self, connection, catalog: str, schema: str) -> dict[str, list[KeyConstraint]]:
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT
                  tc.table_name,
                  tc.constraint_name,
                  k.ordinal_position,
                  k.column_name
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS k
                  ON k.constraint_schema = tc.constraint_schema
                 AND k.constraint_name  = tc.constraint_name
                 AND k.table_schema     = tc.table_schema
                 AND k.table_name       = tc.table_name
                WHERE tc.table_schema = %s
                  AND tc.constraint_type = 'UNIQUE'
                ORDER BY tc.table_name, tc.constraint_name, k.ordinal_position
                """,
                (schema,),
            )
            by_key: dict[tuple[str, str], list[str]] = {}
            for r in cur.fetchall():
                by_key.setdefault((r["TABLE_NAME"], r["CONSTRAINT_NAME"]), []).append(r["COLUMN_NAME"])
        uniques: dict[str, list[KeyConstraint]] = {}
        for (tbl, conname), cols in by_key.items():
            uniques.setdefault(tbl, []).append(KeyConstraint(name=conname, columns=cols, validated=None))
        return uniques

    def collect_table_checks(self, connection, catalog: str, schema: str) -> dict[str, list[CheckConstraint]]:
        """
        Prettify expressions in SQL: remove _utf8mb4 prefixes and backticks.
        """
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT
                  tc.table_name,
                  cc.constraint_name,
                  REPLACE(REPLACE(cc.check_clause, '_utf8mb4', ''), '`','') AS check_clause
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.check_constraints  AS cc
                  ON cc.constraint_schema = tc.constraint_schema
                 AND cc.constraint_name   = tc.constraint_name
                WHERE tc.table_schema = %s
                  AND tc.constraint_type = 'CHECK'
                ORDER BY tc.table_name, cc.constraint_name
                """,
                (schema,),
            )
            checks: dict[str, list[CheckConstraint]] = {}
            for r in cur.fetchall():
                checks.setdefault(r["TABLE_NAME"], []).append(
                    CheckConstraint(
                        name=r["CONSTRAINT_NAME"],
                        expression=r["check_clause"],
                        validated=None,
                    )
                )
            return checks

    def collect_foreign_keys(self, connection, catalog: str, schema: str) -> dict[str, list[ForeignKey]]:
        """
        Build FK objects using KEY_COLUMN_USAGE (child & referenced columns) and REFERENTIAL_CONSTRAINTS (rules).
        """
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT
                  k.table_name,
                  k.constraint_name,
                  k.column_name,
                  k.ordinal_position,
                  k.referenced_table_schema,
                  k.referenced_table_name,
                  k.referenced_column_name,
                  rc.update_rule,
                  rc.delete_rule
                FROM information_schema.key_column_usage AS k
                JOIN information_schema.referential_constraints AS rc
                  ON rc.constraint_schema = k.constraint_schema
                 AND rc.constraint_name   = k.constraint_name
                WHERE k.constraint_schema = %s
                  AND k.referenced_table_schema IS NOT NULL
                ORDER BY k.table_name, k.constraint_name, k.ordinal_position
                """,
                (schema,),
            )
            by_tbl_con: dict[tuple[str, str], dict[str, Any]] = {}
            for r in cur.fetchall():
                key = (r["TABLE_NAME"], r["CONSTRAINT_NAME"])
                entry = by_tbl_con.setdefault(
                    key,
                    {
                        "ref_schema": r["REFERENCED_TABLE_SCHEMA"],
                        "ref_table": r["REFERENCED_TABLE_NAME"],
                        "on_update": (r["UPDATE_RULE"] or "").lower() or None,
                        "on_delete": (r["DELETE_RULE"] or "").lower() or None,
                        "mapping": [],
                    },
                )
                entry["mapping"].append((r["COLUMN_NAME"], r["REFERENCED_COLUMN_NAME"]))

        out: dict[str, list[ForeignKey]] = {}
        for (tbl, conname), info in by_tbl_con.items():
            mapping_objs = [ForeignKeyColumnMap(from_column=f, to_column=t) for f, t in info["mapping"]]
            fk = ForeignKey(
                name=conname,
                mapping=mapping_objs,
                referenced_table=self.qualify(catalog, info["ref_schema"], info["ref_table"]),
                enforced=True,     # InnoDB FKs are enforced
                validated=None,    # MySQL doesn't expose a separate 'validated' flag
                on_update=info["on_update"],
                on_delete=info["on_delete"],
            )
            out.setdefault(tbl, []).append(fk)
        return out

    def collect_indexes(self, connection, catalog: str, schema: str) -> dict[str, list[Index]]:
        """
        Aggregate columns per index in SQL to simplify Python.
        Also de-duplicate indexes that exactly match UNIQUE constraints (same name).
        """
        # Gather UNIQUE constraint names for de-dup
        uniques_by_table: dict[str, set[str]] = {}
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT table_name, constraint_name
                FROM information_schema.table_constraints
                WHERE table_schema = %s AND constraint_type = 'UNIQUE'
                """,
                (schema,),
            )
            for r in cur.fetchall():
                uniques_by_table.setdefault(r["TABLE_NAME"], set()).add(r["CONSTRAINT_NAME"])

        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT
                  table_name,
                  index_name,
                  MIN(non_unique) = 0                   AS is_unique,
                  UPPER(index_type)                     AS index_type,
                  GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR '\x1f') AS cols
                FROM information_schema.statistics
                WHERE table_schema = %s
                  AND UPPER(index_name) <> 'PRIMARY'
                GROUP BY table_name, index_name, index_type
                ORDER BY table_name, index_name
                """,
                (schema,),
            )
            out: dict[str, list[Index]] = {}
            for r in cur.fetchall():
                tbl = r["TABLE_NAME"]
                idx_name = r["INDEX_NAME"]
                # drop indexes that are exactly named like a UNIQUE constraint on the same table
                if bool(r["is_unique"]) and idx_name in uniques_by_table.get(tbl, set()):
                    continue
                cols = (r["cols"] or "")
                col_list = cols.split("\x1f") if cols else []
                out.setdefault(tbl, []).append(
                    Index(
                        name=idx_name,
                        columns=col_list,
                        unique=bool(r["is_unique"]),
                        method=(r["index_type"] or None),
                        predicate=None,
                    )
                )
        return out