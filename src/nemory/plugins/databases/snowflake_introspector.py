from __future__ import annotations

from typing import Any, Mapping

import snowflake.connector
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


class SnowflakeConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="databases/snowflake")
    connection: dict[str, Any] = Field(
        description="Connection parameters for Snowflake. It can contain any of the keys supported by the Snowflake connection library"
    )


class SnowflakeIntrospector(BaseIntrospector[SnowflakeConfigFile]):
    _IGNORED_SCHEMAS = {
        "information_schema",
    }
    _IGNORED_CATALOGS = {
        "STREAMLIT_APPS",
    }
    supports_catalogs = True

    def _connect(self, file_config: SnowflakeConfigFile):
        connection = file_config.connection
        if not isinstance(connection, Mapping):
            raise ValueError("Invalid YAML config: 'connection' must be a mapping of connection parameters")
        # Use qmark so we can bind '?' placeholders in SELECTs.
        snowflake.connector.paramstyle = "qmark"
        return snowflake.connector.connect(**connection)

    # Lowercased dict rows for consistent access
    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        with connection.cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(sql, params or None)
            rows = cur.fetchall()
            return [{(k.lower() if isinstance(k, str) else k): v for k, v in row.items()} for row in rows]

    # ------------------------------- per-catalog config -----------------------------
    def config_for_catalog(self, file_config: SnowflakeConfigFile, catalog: str) -> SnowflakeConfigFile:
        # Reconnect per database by setting 'database' in the connection params.
        try:
            cfg = file_config.model_copy(deep=True)  # pydantic v2
        except AttributeError:
            cfg = file_config.copy(deep=True)        # pydantic v1
        conn = dict(cfg.connection)
        conn["database"] = catalog
        cfg.connection = conn
        return cfg

    # ----------------------------------- catalogs ----------------------------------
    def _get_catalogs(self, connection, file_config: SnowflakeConfigFile) -> list[str]:
        # If a specific DB is configured, scope to it (parity across engines)
        database = file_config.connection.get("database")
        if isinstance(database, str) and database:
            return [database]

        rows = self._fetchall_dicts(connection, "SHOW DATABASES", None)
        # 'name' is one of the SHOW columns; filter out system/shared DBs
        names = [r["name"] for r in rows if r.get("name")]
        return [n for n in names if n.upper() not in self._IGNORED_CATALOGS]

    # ------------------------------------ schemas ----------------------------------
    def get_schemas(self, connection, catalog: str, file_config: SnowflakeConfigFile) -> list[str]:
        rows = self._fetchall_dicts(
            connection,
            "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name",
            None,
        )
        names = [r["schema_name"] for r in rows]
        ignored = {s.lower() for s in (self._ignored_schemas() or [])}
        return [n for n in names if n.lower() not in ignored]

    # -------------------------------- dataset kinds --------------------------------
    def collect_dataset_kinds(self, connection, catalog: str, schema: str) -> dict[str, DatasetKind]:
        rows = self._fetchall_dicts(
            connection,
            """
            SELECT table_name, 'BASE TABLE' AS table_type
            FROM information_schema.tables
            WHERE table_schema = ? AND table_type = 'BASE TABLE'
            UNION ALL
            SELECT table_name, 'VIEW' AS table_type
            FROM information_schema.views
            WHERE table_schema = ?
            """,
            [schema, schema],
        )
        kinds: dict[str, DatasetKind] = {}
        for r in rows:
            kinds[r["table_name"]] = DatasetKind.VIEW if r["table_type"] == "VIEW" else DatasetKind.TABLE
        return kinds

    # ------------------------------------- columns ---------------------------------
    def collect_columns(self, connection, catalog: str, schema: str) -> dict[str, list[DatabaseColumn]]:
        """
        Columns from INFORMATION_SCHEMA.COLUMNS
          - type: data_type
          - nullable: is_nullable ('YES'/'NO')
          - default_expression: column_default
          - description: comment
        Works for both tables and views in the current database.
        """
        rows = self._fetchall_dicts(
            connection,
            """
            SELECT
              table_name,
              column_name,
              data_type,
              is_nullable,
              column_default,
              comment
            FROM information_schema.columns
            WHERE table_schema = ?
            ORDER BY table_name, ordinal_position
            """,
            [schema],
        )
        by_table: dict[str, list[DatabaseColumn]] = {}
        for r in rows:
            nullable = str(r["is_nullable"]).upper() == "YES"
            default_expr = r.get("column_default")
            description = r.get("comment") or None

            # Build column (tolerate older DatabaseColumn signatures)
            try:
                col = DatabaseColumn(
                    name=r["column_name"],
                    type=r["data_type"],
                    nullable=nullable,
                    description=description,
                    default_expression=default_expr,
                    generated=None,  # Snowflake has no table-level computed columns in this demo DDL
                )
            except TypeError:
                col = DatabaseColumn(
                    name=r["column_name"],
                    type=r["data_type"],
                    nullable=nullable,
                    description=description,
                )

            by_table.setdefault(r["table_name"], []).append(col)
        return by_table

    # ---------------------------------- primary keys -------------------------------
    def collect_primary_keys(self, connection, catalog: str, schema: str) -> dict[str, KeyConstraint | None]:
        # get the list of base tables using INFORMATION_SCHEMA (safe & cheap)
        tables = [
            r["table_name"]
            for r in self._fetchall_dicts(
                connection,
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
                [schema],
            )
        ]

        out: dict[str, KeyConstraint | None] = {}
        for tbl in tables:
            rows = self._fetchall_dicts(
                connection,
                f"SHOW PRIMARY KEYS IN TABLE {self._ident(catalog, schema, tbl)}",
                None,
            )
            if not rows:
                continue
            by_seq = sorted(((int(r["key_sequence"]), r["column_name"], r["constraint_name"]) for r in rows), key=lambda t: t[0])
            cols = [c for _, c, _ in by_seq]
            cname = by_seq[0][2]
            out[tbl] = KeyConstraint(name=cname, columns=cols, validated=None)
        return out

    def _ident(self, *parts: str) -> str:
        """
        Quote Snowflake identifiers and join with dots.
        Example: _ident("MYDB","PUBLIC") -> "MYDB"."PUBLIC"
        """

        def q(p: str) -> str:
            p = str(p)
            return '"' + p.replace('"', '""') + '"'

        return ".".join(q(p) for p in parts if p is not None and p != "")

    # ------------------------------- unique constraints ----------------------------
    def collect_unique_constraints(self, connection, catalog: str, schema: str) -> dict[str, list[KeyConstraint]]:
        tables = [
            r["table_name"]
            for r in self._fetchall_dicts(
                connection,
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
                [schema],
            )
        ]
        out: dict[str, list[KeyConstraint]] = {}
        for tbl in tables:
            rows = self._fetchall_dicts(
                connection,
                f"SHOW UNIQUE KEYS IN TABLE {self._ident(catalog, schema, tbl)}",
                None,
            )
            if not rows:
                continue
            grouped: dict[str, list[tuple[int, str]]] = {}
            for r in rows:
                grouped.setdefault(r["constraint_name"], []).append((int(r["key_sequence"]), r["column_name"]))
            for cname, pos_cols in grouped.items():
                cols_sorted = [c for _, c in sorted(pos_cols, key=lambda x: x[0])]
                out.setdefault(tbl, []).append(KeyConstraint(name=cname, columns=cols_sorted, validated=None))
        return out

    # ------------------------------------ checks -----------------------------------
    def collect_table_checks(self, connection, catalog: str, schema: str) -> dict[str, list[CheckConstraint]]:
        """
        Snowflake standard tables do not support CHECK constraints (attempting to create them errors),
        so this returns an empty mapping. If you later add view-based validations or a feature that
        models CHECK-like semantics, you can populate it here.
        """
        return {}

    # -------------------------------- foreign keys ---------------------------------
    def collect_foreign_keys(self, connection, catalog: str, schema: str) -> dict[str, list[ForeignKey]]:
        tables = [
            r["table_name"]
            for r in self._fetchall_dicts(
                connection,
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
                [schema],
            )
        ]

        out: dict[str, list[ForeignKey]] = {}
        for tbl in tables:
            rows = self._fetchall_dicts(
                connection,
                f"SHOW IMPORTED KEYS IN TABLE {self._ident(catalog, schema, tbl)}",
                None,
            )
            if not rows:
                continue

            grouped: dict[str, dict[str, Any]] = {}
            for r in rows:
                fk_name = r.get("name") or r.get("fk_name")
                if not fk_name:
                    continue
                ent = grouped.setdefault(
                    fk_name,
                    {
                        "ref_db": r.get("pk_database_name") or r.get("unique_key_database") or catalog,
                        "ref_sc": r.get("pk_schema_name") or r.get("unique_key_schema") or schema,
                        "ref_tbl": r.get("pk_table_name") or r.get("unique_key_table"),
                        "on_update": (r.get("update_rule") or "").lower() or None,
                        "on_delete": (r.get("delete_rule") or "").lower() or None,
                        "pairs": [],
                    },
                )
                ent["pairs"].append((int(r.get("key_sequence") or 0), r.get("fk_column_name"), r.get("pk_column_name")))

            for cname, info in grouped.items():
                pairs_sorted = sorted(info["pairs"], key=lambda t: t[0])
                mapping = [ForeignKeyColumnMap(from_column=f, to_column=p) for _, f, p in pairs_sorted if f and p]
                if not mapping or not info["ref_tbl"]:
                    continue
                out.setdefault(tbl, []).append(
                    ForeignKey(
                        name=cname,
                        mapping=mapping,
                        referenced_table=self.qualify(
                            info.get("ref_db") or catalog,
                            info.get("ref_sc") or schema,
                            info["ref_tbl"],
                        ),
                        enforced=False,
                        validated=None,
                        on_update=info["on_update"],
                        on_delete=info["on_delete"],
                    )
                )
        return out

    # ------------------------------------- indexes ---------------------------------
    def collect_indexes(self, connection, catalog: str, schema: str) -> dict[str, list[Index]]:
        """
        Snowflake does not expose user-defined secondary indexes (only clustering keys / search optimization).
        Return an empty mapping for simplicity and cross-engine consistency.
        """
        return {}

    def _get_boolish(self, row: dict, *keys: str) -> bool | None:
        """
        Extract a boolean-ish flag from rows that may expose 'YES/NO', 'Y/N', 'TRUE/FALSE'.
        Returns None if no key is present.
        """
        for k in keys:
            if k in row and row[k] is not None:
                v = str(row[k]).strip().upper()
                return v in ("Y", "YES", "TRUE", "T", "1")
        return None