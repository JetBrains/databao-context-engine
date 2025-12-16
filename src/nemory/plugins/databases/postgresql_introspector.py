from typing import Annotated, Any

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

from nemory.pluginlib.config_properties import ConfigPropertyAnnotation
from nemory.plugins.base_db_plugin import BaseDatabaseConfigFile
from nemory.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from nemory.plugins.databases.databases_types import (
    CheckConstraint,
    DatabaseColumn,
    DatabasePartitionInfo,
    DatasetKind,
    ForeignKey,
    ForeignKeyColumnMap,
    Index,
    KeyConstraint,
)


class PostgresConnectionProperties(BaseModel):
    host: Annotated[str, ConfigPropertyAnnotation(default_value="localhost", required=True)]
    port: int | None = None
    database: str | None = None
    user: str | None = None
    password: str | None = None
    additional_properties: dict[str, Any] = {}


class PostgresConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="databases/postgres")
    connection: PostgresConnectionProperties


class PostgresqlIntrospector(BaseIntrospector[PostgresConfigFile]):
    _IGNORED_SCHEMAS = {"information_schema", "pg_catalog", "pg_toast"}

    supports_catalogs = True

    def _connect(self, file_config: PostgresConfigFile):
        connection_string = self._create_connection_string_for_config(file_config.connection)

        return psycopg.connect(connection_string)

    def _get_catalogs(self, connection: Connection, file_config: PostgresConfigFile) -> list[str]:
        """
        Return the list of catalogs (postgres databases) to introspect.
        """
        # database = file_config.connection.database
        # if database is not None:
        #     return [database]

        catalog_results = connection.execute(
            "SELECT datname FROM pg_catalog.pg_database WHERE datistemplate = false"
        ).fetchall()

        return [row[0] for row in catalog_results]

    def collect_partitions_for_schema(self, connection: Connection, catalog: str, schema: str) -> SQLQuery:
        """
        Return partition metadata per partitioned table in the schema.
        """
        sql = """
        WITH partitions AS
                 (SELECT parentrel.oid, array_agg(childrel.relname) as partition_tables
                  FROM pg_catalog.pg_class parentrel
                           JOIN pg_catalog.pg_inherits inh ON inh.inhparent = parentrel.oid
                           JOIN pg_catalog.pg_class childrel ON inh.inhrelid = childrel.oid
                  GROUP BY parentrel.oid)
        SELECT rel.relname            as table_name,
               CASE part.partstrat
                   WHEN 'h' THEN 'hash partitioned'
                   WHEN 'l' THEN 'list partitioned'
                   WHEN 'r' THEN 'range partitioned'
                   END                AS partitioning_strategy,
               array_agg(att.attname) as columns_in_partition_key,
               partitions.partition_tables
        FROM pg_catalog.pg_partitioned_table part
                 JOIN pg_catalog.pg_class rel ON part.partrelid = rel.oid
                 JOIN pg_catalog.pg_namespace nsp ON rel.relnamespace = nsp.oid
                 JOIN pg_catalog.pg_attribute att ON att.attrelid = rel.oid AND att.attnum = ANY (part.partattrs)
                 JOIN partitions ON partitions.oid = rel.oid
        WHERE nsp.nspname = %s
        GROUP BY rel.relname, part.partstrat, partitions.partition_tables
        """
        out: dict[str, DatabasePartitionInfo] = {}
        with connection.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, (schema,))
            for row in cur.fetchall():
                out[row["table_name"]] = DatabasePartitionInfo(
                    meta={
                        "partitioning_strategy": row["partitioning_strategy"],
                        "columns_in_partition_key": row["columns_in_partition_key"],
                    },
                    partition_tables=row["partition_tables"],
                )
        return out

    def get_schemas(self, connection, catalog: str, file_config: PostgresConfigFile) -> list[str]:
        """
        Return all schemas in the current catalog.
        """
        sql = """
            SELECT
                nspname AS schema_name
            FROM
                pg_namespace
            ORDER BY
                1
        """
        rows = connection.execute(sql).fetchall()
        return [r[0] for r in rows]

    def collect_dataset_kinds(self, connection: Connection, catalog: str, schema: str) -> dict[str, DatasetKind]:
        """Return a mapping of the relation name -> DatasetKind.
        - 'r' (table), 'p' (partitioned table), 'v' (view), 'm' (materialized view), 'f' (foreign table)
        """
        sql = """
            SELECT
                c.relname AS relname,
                c.relkind
            FROM
                pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE
                n.nspname = %s
                AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
        """
        with connection.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, (schema,))
            kinds: dict[str, DatasetKind] = {}
            for row in cur.fetchall():
                rk = row["relkind"]
                if rk in ("r", "p"):
                    kinds[row["relname"]] = DatasetKind.TABLE
                elif rk == "v":
                    kinds[row["relname"]] = DatasetKind.VIEW
                elif rk == "m":
                    kinds[row["relname"]] = DatasetKind.MATERIALIZED_VIEW
                elif rk == "f":
                    kinds[row["relname"]] = DatasetKind.EXTERNAL_TABLE
            return kinds

    def collect_columns(self, connection: Connection, catalog: str, schema: str) -> dict[str, list[DatabaseColumn]]:
        """
        Collect columns metadata for all relations in a schema.
        """
        sql = """
            SELECT
                c.relname                                  AS table_name,
                a.attname                                  AS column_name,
                NOT a.attnotnull                           AS is_nullable,
                format_type(a.atttypid, a.atttypmod)       AS type_str,
                col_description(c.oid, a.attnum)           AS column_comment,
                pg_get_expr(ad.adbin, ad.adrelid)          AS default_expr,
                a.attidentity                              AS attidentity,
                a.attgenerated                             AS attgenerated
            FROM 
                pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_attribute a ON a.attrelid = c.oid
                LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
            WHERE 
                n.nspname = %s
                AND c.relkind IN ('r','p','v','m','f')
                AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY c.relname, a.attnum
        """
        columns_by_table: dict[str, list[DatabaseColumn]] = {}
        with connection.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, (schema,))
            for row in cur.fetchall():
                table = row["table_name"]

                generated: str | None
                if row["attidentity"] in ("a", "d"):
                    generated = "identity"
                elif row["attgenerated"] == "s":
                    generated = "computed"
                else:
                    generated = None

                column = DatabaseColumn(
                    name=row["column_name"],
                    type=row["type_str"],
                    nullable=bool(row["is_nullable"]),
                    description=row["column_comment"],
                    default_expression=row["default_expr"],
                    generated=generated,
                )
                columns_by_table.setdefault(table, []).append(column)
        return columns_by_table

    def collect_primary_keys(
        self, connection: Connection, catalog: str, schema: str
    ) -> dict[str, KeyConstraint | None]:
        """
        Collects primary key constraints for all tables in a schema.
        """
        sql = """
            SELECT
                c.relname           AS table_name,
                con.conname         AS constraint_name,
                con.convalidated    AS validated,
                k.pos               AS pos,
                a.attname           AS col
            FROM
                pg_constraint con
                JOIN pg_class c ON c.oid = con.conrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN LATERAL generate_subscripts(con.conkey, 1) AS k(pos) ON TRUE
                JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = con.conkey[k.pos]
            WHERE
                n.nspname = %s
                AND con.contype = 'p'
            ORDER BY
                c.relname,
                k.pos
        """
        pks: dict[str, KeyConstraint | None] = {}
        with connection.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, (schema,))
            current_table = None
            cols: list[str] = []
            name: str | None = None
            validated: bool | None = None
            for row in cur.fetchall():
                table = row["table_name"]
                if current_table is None:
                    current_table = table
                    name = row["constraint_name"]
                    validated = bool(row["validated"]) if row["validated"] is not None else None
                elif table != current_table:
                    pks[current_table] = KeyConstraint(name=name, columns=cols, validated=validated)
                    current_table = table
                    cols = []
                    name = row["constraint_name"]
                    validated = bool(row["validated"]) if row["validated"] is not None else None

                cols.append(row["col"])
            if current_table is not None:
                pks[current_table] = KeyConstraint(name=name, columns=cols, validated=validated)
        return pks

    def collect_unique_constraints(
        self, connection: Connection, catalog: str, schema: str
    ) -> dict[str, list[KeyConstraint]]:
        """
        Collect unique (non primary key constraints) for all tables in a schema.
        """
        sql = """
            SELECT c.relname        AS table_name,
                   con.conname      AS constraint_name,
                   con.convalidated AS validated,
                   k.pos            AS pos,
                   a.attname        AS col
            FROM 
                pg_constraint con
                JOIN pg_class c ON c.oid = con.conrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN LATERAL generate_subscripts(con.conkey, 1) AS k(pos) ON TRUE
                JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = con.conkey[k.pos]
            WHERE 
                n.nspname = %s 
                AND con.contype = 'u'
            ORDER BY 
                c.relname, 
                constraint_name, 
                k.pos
        """
        uniques: dict[str, list[KeyConstraint]] = {}
        with connection.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, (schema,))
            by_tbl_con: dict[tuple[str, str], dict[str, Any]] = {}
            for row in cur.fetchall():
                key = (row["table_name"], row["constraint_name"])
                entry = by_tbl_con.setdefault(key, {"cols": [], "validated": row["validated"]})
                entry["cols"].append(row["col"])
            for (tbl, conname), info in by_tbl_con.items():
                uniques.setdefault(tbl, []).append(
                    KeyConstraint(
                        name=conname,
                        columns=info["cols"],
                        validated=bool(info["validated"]) if info["validated"] is not None else None,
                    )
                )
        return uniques

    def collect_table_checks(
        self, connection: Connection, catalog: str, schema: str
    ) -> dict[str, list[CheckConstraint]]:
        """
        Collect check constraints for all tables in a schema.
        """
        sql = """
            SELECT 
                c.relname                           AS table_name,
                con.conname                         AS constraint_name,
                con.convalidated                    AS validated,
                pg_get_constraintdef(con.oid, true) AS expr
            FROM 
                pg_constraint con
                JOIN pg_class c ON c.oid = con.conrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE 
                n.nspname = %s 
                AND con.contype = 'c'
            ORDER BY 
                c.relname, 
                con.conname
        """
        checks: dict[str, list[CheckConstraint]] = {}
        with connection.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, (schema,))
            for row in cur.fetchall():
                chk = CheckConstraint(
                    name=row["constraint_name"],
                    expression=row["expr"],
                    validated=bool(row["validated"]) if row["validated"] is not None else None,
                )
                checks.setdefault(row["table_name"], []).append(chk)
        return checks

    def collect_foreign_keys(self, connection: Connection, catalog: str, schema: str) -> dict[str, list[ForeignKey]]:
        """
        Collect foreign key constraints for all tables in a schema
        """
        sql = """
            SELECT
                child.relname       AS table_name,
                con.conname         AS constraint_name,
                con.convalidated    AS validated,
                con.confdeltype     AS del_action,
                con.confupdtype     AS upd_action,
                refns.nspname       AS ref_schema,
                ref.relname         AS ref_table,
                ord.n               AS pos,
                atc.attname         AS from_col,
                atr.attname         AS to_col
            FROM 
                pg_constraint con
                JOIN pg_class child     ON child.oid = con.conrelid
                JOIN pg_namespace cns   ON cns.oid   = child.relnamespace
                JOIN pg_class ref       ON ref.oid   = con.confrelid
                JOIN pg_namespace refns ON refns.oid = ref.relnamespace
                JOIN LATERAL generate_subscripts(con.conkey, 1) AS ord(n) ON TRUE
                JOIN pg_attribute atc ON atc.attrelid = child.oid AND atc.attnum = con.conkey[ord.n]
                JOIN pg_attribute atr ON atr.attrelid = ref.oid   AND atr.attnum = con.confkey[ord.n]
            WHERE 
                cns.nspname = %s AND con.contype = 'f'
            ORDER BY 
                child.relname, 
                con.conname, ord.n
        """
        action_map = {
            "a": "no_action",
            "r": "restrict",
            "c": "cascade",
            "n": "set_null",
            "d": "set_default",
        }
        by_tbl_con: dict[tuple[str, str], dict[str, Any]] = {}
        with connection.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, (schema,))
            for row in cur.fetchall():
                key = (row["table_name"], row["constraint_name"])
                entry = by_tbl_con.setdefault(
                    key,
                    {
                        "mapping": [],
                        "ref_schema": row["ref_schema"],
                        "ref_table": row["ref_table"],
                        "validated": row["validated"],
                        "on_delete": action_map.get(row["del_action"], None),
                        "on_update": action_map.get(row["upd_action"], None),
                    },
                )
                entry["mapping"].append((row["from_col"], row["to_col"]))

        fks: dict[str, list[ForeignKey]] = {}
        for (tbl, conname), info in by_tbl_con.items():
            mapping_objs = [ForeignKeyColumnMap(from_column=f, to_column=t) for (f, t) in info["mapping"]]
            fk = ForeignKey(
                name=conname,
                mapping=mapping_objs,
                referenced_table=self.qualify(catalog, info["ref_schema"], info["ref_table"]),
                enforced=True,
                validated=bool(info["validated"]) if info["validated"] is not None else None,
                on_update=info["on_update"],
                on_delete=info["on_delete"],
            )
            fks.setdefault(tbl, []).append(fk)
        return fks

    def collect_indexes(self, connection: Connection, catalog: str, schema: str) -> dict[str, list[Index]]:
        """
        Collect index definitions for all tables in a schema.
        """
        sql = """
            SELECT
                tbl.relname                                 AS table_name,
                idx.relname                                 AS index_name,
                i.indisunique                               AS is_unique,
                am.amname                                   AS method,
                pg_get_expr(i.indpred, i.indrelid)          AS predicate,
                k.pos                                       AS key_pos,
                pg_get_indexdef(i.indexrelid, k.pos, true)  AS key_expr
            FROM 
                pg_index i
                JOIN pg_class tbl ON tbl.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = tbl.relnamespace
                JOIN pg_class idx ON idx.oid = i.indexrelid
                JOIN pg_am am ON am.oid = idx.relam
                JOIN LATERAL generate_subscripts(i.indkey, 1) AS k(pos) ON TRUE
            WHERE 
                n.nspname = %s
            ORDER BY 
                tbl.relname, 
                idx.relname, 
                k.pos
        """
        by_tbl_idx: dict[tuple[str, str], dict[str, Any]] = {}
        with connection.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, (schema,))
            for row in cur.fetchall():
                key = (row["table_name"], row["index_name"])
                entry = by_tbl_idx.setdefault(
                    key,
                    {
                        "unique": bool(row["is_unique"]),
                        "method": row["method"],
                        "predicate": row["predicate"],
                        "cols": [],
                    },
                )
                entry["cols"].append(row["key_expr"])

        indexes: dict[str, list[Index]] = {}
        for (tbl, idxname), info in by_tbl_idx.items():
            indexes.setdefault(tbl, []).append(
                Index(
                    name=idxname,
                    columns=info["cols"],
                    unique=info["unique"],
                    method=info["method"],
                    predicate=info["predicate"],
                )
            )
        return indexes

    def _create_connection_string_for_config(self, connection_config: PostgresConnectionProperties) -> str:
        def _escape_pg_value(value: str) -> str:
            escaped = value.replace("\\", "\\\\").replace("'", "\\'")
            return f"'{escaped}'"

        host = connection_config.host
        if host is None:
            raise ValueError("A host must be provided to connect to the PostgreSQL database.")

        connection_parts = {
            "host": host,
            "port": connection_config.port or 5432,
            "dbname": connection_config.database,
            "user": connection_config.user,
            "password": connection_config.password,
        }
        connection_parts.update(connection_config.additional_properties)

        connection_string = " ".join(
            f"{k}={_escape_pg_value(str(v))}" for k, v in connection_parts.items() if v is not None
        )
        return connection_string

    def config_for_catalog(self, file_config: PostgresConfigFile, catalog: str) -> PostgresConfigFile:
        """
        PostgreSQL requires a separate connection per database (catalog).
        Return a deep-copied config with .connection.database set to `catalog`.
        """
        cfg = file_config.model_copy(deep=True)
        cfg.connection.database = catalog
        return cfg
