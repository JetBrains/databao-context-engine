from __future__ import annotations

from typing import Any, Mapping

from mssql_python import connect  # type: ignore
from pydantic import Field

from nemory.plugins.base_db_plugin import BaseDatabaseConfigFile
from nemory.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from nemory.plugins.databases.databases_types import (
    DatabaseColumn,
    ForeignKey,
    ForeignKeyColumnMap,
    Index,
    CheckConstraint,
    KeyConstraint,
    DatasetKind,
)


class MSSQLConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="databases/mssql")
    connection: dict[str, Any] = Field(
        description="Connection parameters for the Microsoft Server SQL database. It can contain any of the keys supported by the Microsoft Server connection library"
    )


class MSSQLIntrospector(BaseIntrospector[MSSQLConfigFile]):
    _IGNORED_SCHEMAS = {
        "sys",
        "information_schema",
        "db_accessadmin",
        "db_backupoperator",
        "db_datareader",
        "db_datawriter",
        "db_ddladmin",
        "db_denydatareader",
        "db_denydatawriter",
        "db_owner",
        "db_securityadmin",
    }
    _IGNORED_CATALOGS = (
        "master",
        "model",
        "msdb",
        "tempdb",
    )
    supports_catalogs = True

    def _connect(self, file_config: MSSQLConfigFile):
        connection = file_config.connection
        if not isinstance(connection, Mapping):
            raise ValueError("Invalid YAML config: 'connection' must be a mapping of connection parameters")

        connection_string = self._create_connection_string_for_config(connection)
        return connect(connection_string)

    def _get_catalogs(self, connection, file_config: MSSQLConfigFile) -> list[str]:
        # If a specific DB is configured, scope to it (parity with other engines)
        database = file_config.connection.get("database")
        if isinstance(database, str) and database:
            return [database]

        rows = self._fetchall_dicts(
            connection,
            """
            SELECT name
            FROM sys.databases
            WHERE state = 0 /* ONLINE */
            ORDER BY name
            """,
            None,
        )
        all_catalogs = [r["name"] for r in rows]
        return [c for c in all_catalogs if c.lower() not in self._IGNORED_CATALOGS]

    def config_for_catalog(self, file_config: MSSQLConfigFile, catalog: str) -> MSSQLConfigFile:
        # Reconnect per catalog by setting 'database' in the DSN
        try:
            cfg = file_config.model_copy(deep=True)  # pydantic v2
        except AttributeError:
            cfg = file_config.copy(deep=True)        # pydantic v1
        conn = dict(cfg.connection)
        conn["database"] = catalog
        cfg.connection = conn
        return cfg

    # ----------------------------------- schemas ---------------------------------
    def get_schemas(self, connection, catalog: str, file_config: MSSQLConfigFile) -> list[str]:
        rows = self._fetchall_dicts(
            connection,
            "SELECT name FROM sys.schemas ORDER BY name",
            None,
        )
        names = [r["name"] for r in rows]
        ignored = {s.lower() for s in (self._ignored_schemas() or [])}
        return [n for n in names if n.lower() not in ignored]

    # ------------------------------ dataset kinds --------------------------------
    def collect_dataset_kinds(self, connection, catalog: str, schema: str) -> dict[str, DatasetKind]:
        rows = self._fetchall_dicts(
            connection,
            """
            SELECT t.name AS table_name, 'BASE TABLE' AS table_type
            FROM sys.tables AS t
            JOIN sys.schemas AS s ON s.schema_id = t.schema_id
            WHERE s.name = ?
            UNION ALL
            SELECT v.name AS table_name, 'VIEW' AS table_type
            FROM sys.views AS v
            JOIN sys.schemas AS s ON s.schema_id = v.schema_id
            WHERE s.name = ?
            """,
            [schema, schema],
        )
        kinds: dict[str, DatasetKind] = {}
        for r in rows:
            kinds[r["table_name"]] = DatasetKind.VIEW if (r["table_type"] == "VIEW") else DatasetKind.TABLE
        return kinds

    # ---------------------------------- columns ----------------------------------
    def collect_columns(self, connection, catalog: str, schema: str) -> dict[str, list[DatabaseColumn]]:
        """
        Collect columns for both tables and views:
        - type: sys.types.name (base type)
        - nullable: sys.columns.is_nullable
        - default_expression: sys.default_constraints.definition (None for views)
        - generated: 'computed' if sys.computed_columns.definition is present
        - description: column MS_Description extended property (if any)
        """
        rows = self._fetchall_dicts(
            connection,
            """
            SELECT
              s.name                           AS schema_name,
              o.name                           AS table_name,
              c.column_id,
              c.name                           AS column_name,
              ty.name                          AS data_type,
              c.is_nullable,
              dc.definition                    AS default_definition,
              cc.definition                    AS computed_definition,
              CAST(ep.value AS NVARCHAR(4000)) AS column_comment
            FROM sys.objects AS o
            JOIN sys.schemas AS s ON s.schema_id = o.schema_id
            JOIN sys.columns AS c ON c.object_id = o.object_id
            JOIN sys.types   AS ty ON ty.user_type_id = c.user_type_id
            LEFT JOIN sys.default_constraints AS dc
              ON dc.object_id = c.default_object_id
            LEFT JOIN sys.computed_columns AS cc
              ON cc.object_id = c.object_id AND cc.column_id = c.column_id
            LEFT JOIN sys.extended_properties AS ep
              ON ep.class = 1
             AND ep.major_id = c.object_id
             AND ep.minor_id = c.column_id
             AND ep.name = 'MS_Description'
            WHERE s.name = ?
              AND o.type IN ('U','V')  -- tables and views
            ORDER BY o.name, c.column_id
            """,
            [schema],
        )
        by_table: dict[str, list[DatabaseColumn]] = {}
        for r in rows:
            is_generated = bool(r.get("computed_definition"))
            default_expr = r.get("computed_definition") if is_generated else r.get("default_definition")
            # Build column (tolerate older DatabaseColumn signatures)
            try:
                col = DatabaseColumn(
                    name=r["column_name"],
                    type=r["data_type"],
                    nullable=bool(r["is_nullable"]),
                    description=(r.get("column_comment") or None),
                    default_expression=default_expr,
                    generated="computed" if is_generated else None,
                )
            except TypeError:
                col = DatabaseColumn(
                    name=r["column_name"],
                    type=r["data_type"],
                    nullable=bool(r["is_nullable"]),
                    description=(r.get("column_comment") or None),
                )
            by_table.setdefault(r["table_name"], []).append(col)
        return by_table
    # ------------------------------- primary keys --------------------------------
    def collect_primary_keys(self, connection, catalog: str, schema: str) -> dict[str, KeyConstraint | None]:
        rows = self._fetchall_dicts(
            connection,
            """
            SELECT
              t.name           AS table_name,
              kc.name          AS constraint_name,
              ic.key_ordinal   AS ordinal,
              c.name           AS column_name
            FROM sys.key_constraints AS kc
            JOIN sys.tables AS t
              ON t.object_id = kc.parent_object_id
            JOIN sys.schemas AS s
              ON s.schema_id = t.schema_id
            JOIN sys.indexes AS i
              ON i.object_id = t.object_id AND i.index_id = kc.unique_index_id
            JOIN sys.index_columns AS ic
              ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns AS c
              ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE s.name = ? AND kc.type = 'PK'
            ORDER BY t.name, kc.name, ic.key_ordinal
            """,
            [schema],
        )
        by_tbl: dict[str, dict[str, Any]] = {}
        for r in rows:
            entry = by_tbl.setdefault(r["table_name"], {"name": r["constraint_name"], "cols": []})
            entry["cols"].append(r["column_name"])
        return {tbl: KeyConstraint(name=v["name"], columns=v["cols"], validated=None) for tbl, v in by_tbl.items()}

    # ------------------------------ unique constraints ----------------------------
    def collect_unique_constraints(self, connection, catalog: str, schema: str) -> dict[str, list[KeyConstraint]]:
        rows = self._fetchall_dicts(
            connection,
            """
            SELECT
              t.name           AS table_name,
              kc.name          AS constraint_name,
              ic.key_ordinal   AS ordinal,
              c.name           AS column_name
            FROM sys.key_constraints AS kc
            JOIN sys.tables AS t
              ON t.object_id = kc.parent_object_id
            JOIN sys.schemas AS s
              ON s.schema_id = t.schema_id
            JOIN sys.indexes AS i
              ON i.object_id = t.object_id AND i.index_id = kc.unique_index_id
            JOIN sys.index_columns AS ic
              ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns AS c
              ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE s.name = ? AND kc.type = 'UQ'
            ORDER BY t.name, kc.name, ic.key_ordinal
            """,
            [schema],
        )
        by_key: dict[tuple[str, str], list[str]] = {}
        for r in rows:
            by_key.setdefault((r["table_name"], r["constraint_name"]), []).append(r["column_name"])
        out: dict[str, list[KeyConstraint]] = {}
        for (tbl, cname), cols in by_key.items():
            out.setdefault(tbl, []).append(KeyConstraint(name=cname, columns=cols, validated=None))
        return out

    # --------------------------------- checks ------------------------------------
    def collect_table_checks(self, connection, catalog: str, schema: str) -> dict[str, list[CheckConstraint]]:
        rows = self._fetchall_dicts(
            connection,
            """
            SELECT
              t.name       AS table_name,
              cc.name      AS constraint_name,
              cc.definition AS definition
            FROM sys.check_constraints AS cc
            JOIN sys.tables AS t ON t.object_id = cc.parent_object_id
            JOIN sys.schemas AS s ON s.schema_id = t.schema_id
            WHERE s.name = ?
            ORDER BY t.name, cc.name
            """,
            [schema],
        )
        checks: dict[str, list[CheckConstraint]] = {}
        for r in rows:
            checks.setdefault(r["table_name"], []).append(
                CheckConstraint(name=r["constraint_name"], expression=r["definition"], validated=None)
            )
        return checks

    # ---------------------------------- FKs --------------------------------------
    def collect_foreign_keys(self, connection, catalog: str, schema: str) -> dict[str, list[ForeignKey]]:
        rows = self._fetchall_dicts(
            connection,
            """
            SELECT
              s.name                 AS schema_name,
              t.name                 AS table_name,
              fk.name                AS constraint_name,
              sref.name              AS ref_schema,
              tref.name              AS ref_table,
              fkc.constraint_column_id AS pos,
              cp.name                AS from_col,
              cr.name                AS to_col,
              fk.delete_referential_action_desc AS delete_rule,
              fk.update_referential_action_desc AS update_rule
            FROM sys.foreign_keys AS fk
            JOIN sys.tables AS t               ON t.object_id = fk.parent_object_id
            JOIN sys.schemas AS s              ON s.schema_id = t.schema_id
            JOIN sys.tables AS tref            ON tref.object_id = fk.referenced_object_id
            JOIN sys.schemas AS sref           ON sref.schema_id = tref.schema_id
            JOIN sys.foreign_key_columns AS fkc ON fkc.constraint_object_id = fk.object_id
            JOIN sys.columns AS cp             ON cp.object_id = fkc.parent_object_id AND cp.column_id = fkc.parent_column_id
            JOIN sys.columns AS cr             ON cr.object_id = fkc.referenced_object_id AND cr.column_id = fkc.referenced_column_id
            WHERE s.name = ?
            ORDER BY t.name, fk.name, fkc.constraint_column_id
            """,
            [schema],
        )
        by_tbl_con: dict[tuple[str, str], dict[str, Any]] = {}
        for r in rows:
            key = (r["table_name"], r["constraint_name"])
            entry = by_tbl_con.setdefault(
                key,
                {
                    "ref_schema": r["ref_schema"],
                    "ref_table": r["ref_table"],
                    "on_update": (r["update_rule"] or "").lower() or None,
                    "on_delete": (r["delete_rule"] or "").lower() or None,
                    "mapping": [],
                },
            )
            entry["mapping"].append((r["from_col"], r["to_col"]))

        out: dict[str, list[ForeignKey]] = {}
        for (tbl, conname), info in by_tbl_con.items():
            mapping_objs = [ForeignKeyColumnMap(from_column=f, to_column=t) for f, t in info["mapping"]]
            fk = ForeignKey(
                name=conname,
                mapping=mapping_objs,
                referenced_table=self.qualify(catalog, info["ref_schema"], info["ref_table"]),
                enforced=True,     # SQL Server enforces FK constraints
                validated=None,
                on_update=info["on_update"],
                on_delete=info["on_delete"],
            )
            out.setdefault(tbl, []).append(fk)
        return out

    # -------------------------------- indexes ------------------------------------
    def collect_indexes(self, connection, catalog: str, schema: str) -> dict[str, list[Index]]:
        """
        Secondary indexes from sys.indexes/statistics.
        Excludes PRIMARY KEY / UNIQUE CONSTRAINT storage (i.is_primary_key/is_unique_constraint).
        Captures filtered index predicate via i.filter_definition.
        """
        rows = self._fetchall_dicts(
            connection,
            """
            SELECT
              t.name                 AS table_name,
              i.name                 AS index_name,
              i.is_unique            AS is_unique,
              i.type_desc            AS type_desc,
              i.filter_definition    AS filter_definition,
              ic.key_ordinal         AS key_ordinal,
              c.name                 AS column_name
            FROM sys.indexes AS i
            JOIN sys.tables  AS t  ON t.object_id = i.object_id
            JOIN sys.schemas AS s  ON s.schema_id = t.schema_id
            JOIN sys.index_columns AS ic
              ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
            JOIN sys.columns AS c
              ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE s.name = ?
              AND i.is_hypothetical = 0
              AND i.is_primary_key = 0
              AND i.is_unique_constraint = 0
            ORDER BY t.name, i.name, ic.key_ordinal
            """,
            [schema],
        )
        by_tbl_idx: dict[tuple[str, str], dict[str, Any]] = {}
        for r in rows:
            key = (r["table_name"], r["index_name"])
            entry = by_tbl_idx.setdefault(
                key,
                {
                    "unique": bool(r["is_unique"]),
                    "method": (r["type_desc"] or "").upper() or None,  # CLUSTERED/NONCLUSTERED
                    "predicate": r.get("filter_definition") or None,
                    "cols": [],
                },
            )
            entry["cols"].append(r["column_name"])

        out: dict[str, list[Index]] = {}
        for (tbl, idx_name), info in by_tbl_idx.items():
            out.setdefault(tbl, []).append(
                Index(
                    name=idx_name,
                    columns=info["cols"],
                    unique=info["unique"],
                    method=info["method"],
                    predicate=info["predicate"],
                )
            )
        return out

    # --------------------------------- helpers -----------------------------------
    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        with connection.cursor() as cursor:
            cursor.execute(sql, params or ())
            if not cursor.description:
                return []
            cols = [d[0] for d in cursor.description]
            # Normalize to lower-case keys for consistent field access in this adapter
            cols = [c.lower() if isinstance(c, str) else c for c in cols]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def _create_connection_string_for_config(self, file_config: Mapping[str, Any]) -> str:
        def _escape_odbc_value(value: str) -> str:
            return "{" + value.replace("}", "}}").replace("{", "{{") + "}"

        host = file_config.get("host")
        if not host:
            raise ValueError("A host must be provided to connect to the MSSQL database.")

        port = file_config.get("port", 1433)
        instance = file_config.get("instanceName")
        if instance:
            server_part = f"{host}\\{instance}"
        else:
            server_part = f"{host},{port}"

        database = file_config.get("database")
        user = file_config.get("user")
        password = file_config.get("password")

        connection_parts = {
            "server": _escape_odbc_value(server_part),
            "database": _escape_odbc_value(str(database)) if database is not None else None,
            "uid": _escape_odbc_value(str(user)) if user is not None else None,
            "pwd": _escape_odbc_value(str(password)) if password is not None else None,
            "encrypt": file_config.get("encrypt"),
            "trust_server_certificate": "yes" if file_config.get("trust_server_certificate") else None,
        }

        connection_string = ";".join(f"{k}={v}" for k, v in connection_parts.items() if v is not None)
        return connection_string