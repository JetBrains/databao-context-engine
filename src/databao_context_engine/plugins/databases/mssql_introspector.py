from __future__ import annotations

from typing import Any, Mapping

from mssql_python import connect  # type: ignore[import-untyped]
from pydantic import Field

from databao_context_engine.plugins.base_db_plugin import BaseDatabaseConfigFile
from databao_context_engine.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from databao_context_engine.plugins.databases.databases_types import DatabaseTable
from databao_context_engine.plugins.databases.table_builder import TableBuilder


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

    def _connect_to_catalog(self, file_config: MSSQLConfigFile, catalog: str):
        base_cfg = file_config.connection or {}
        cfg_for_db: dict[str, Any] = dict(base_cfg)
        cfg_for_db["database"] = catalog

        connection_string = self._create_connection_string_for_config(cfg_for_db)
        return connect(connection_string)

    def _get_catalogs(self, connection, file_config: MSSQLConfigFile) -> list[str]:
        database = file_config.connection.get("database")
        if isinstance(database, str) and database:
            return [database]

        rows = self._fetchall_dicts(connection, "SELECT name FROM sys.databases", None)
        all_catalogs = [row["name"] for row in rows]
        return [catalog for catalog in all_catalogs if catalog not in self._IGNORED_CATALOGS]

    def _sql_list_schemas(self, catalogs: list[str] | None) -> SQLQuery:
        if not catalogs:
            return SQLQuery("SELECT schema_name, catalog_name FROM information_schema.schemata", None)

        parts = []
        for catalog in catalogs:
            parts.append(f"SELECT schema_name, catalog_name FROM {catalog}.information_schema.schemata")
        return SQLQuery(" UNION ALL ".join(parts), None)

    _USE_BATCH = True

    def collect_schema_model(self, connection, catalog: str, schema: str) -> list[DatabaseTable] | None:
        comps = self._component_queries()

        schema_lit = self._quote_literal(schema)
        stmts = [sql.replace("{SCHEMA}", schema_lit) for sql in comps.values()]
        batch_prefix = "SET NOCOUNT ON; SET XACT_ABORT ON;"
        batch = batch_prefix + "\n" + ";\n".join(s.rstrip().rstrip(";") for s in stmts) + ";"

        results: dict[str, list[dict]] = {name: [] for name in comps}
        with connection.cursor() as cur:
            cur.execute(batch)
            for ix, name in enumerate(comps.keys(), start=1):
                try:
                    rows: list[dict] = []
                    if cur.description:
                        cols = [c[0].lower() for c in cur.description]
                        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
                    results[name] = rows
                except Exception as e:
                    raise RuntimeError(f"Failed reading component #{ix} '{name}'") from e
                if ix < len(comps):
                    ok = cur.nextset()
                    if not ok:
                        raise RuntimeError(f"Batch ended early after component #{ix} '{name}'")

        return TableBuilder.build_from_components(
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
            "uq": self._sql_uniques(),
            "checks": self._sql_checks(),
            "fks": self._sql_foreign_keys(),
            "idx": self._sql_indexes(),
        }

    def _sql_relations(self) -> str:
        return r"""
            SELECT
                t.name AS table_name,
                'table' AS kind,
                CAST(ep.value AS nvarchar(4000)) AS description
            FROM 
                sys.tables t
                JOIN sys.schemas s ON s.schema_id = t.schema_id
                LEFT JOIN sys.extended_properties ep 
                          ON ep.major_id = t.object_id AND ep.minor_id = 0 AND ep.class = 1 AND ep.name = 'MS_Description'
            WHERE 
                s.name = {SCHEMA}
            UNION ALL
            SELECT
                v.name                    AS table_name,
                'view'                    AS kind,
                CAST(ep.value AS nvarchar(4000)) AS description
            FROM 
                sys.views v
                JOIN sys.schemas s ON s.schema_id = v.schema_id
                LEFT JOIN sys.extended_properties ep 
                          ON ep.major_id = v.object_id AND ep.minor_id = 0 AND ep.class = 1 AND ep.name = 'MS_Description'
            WHERE 
                s.name = {SCHEMA}
            UNION ALL
            SELECT
                et.name AS table_name,
                'external_table' AS kind,
                CAST(ep.value AS nvarchar(4000)) AS description
            FROM 
                sys.external_tables et
                JOIN sys.schemas s ON s.schema_id = et.schema_id
                LEFT JOIN sys.extended_properties ep 
                          ON ep.major_id = et.object_id AND ep.minor_id = 0 AND ep.class = 1 AND ep.name = 'MS_Description'
            WHERE 
                s.name = {SCHEMA}
            ORDER BY 
                table_name;
        """

    # TODO: simplify case when for datatype
    def _sql_columns(self) -> str:
        return r"""
            SELECT
                o.name AS table_name,
                c.name AS column_name,
                c.column_id AS ordinal_position,
                CASE
                    WHEN t.name IN ('varchar','char','varbinary','binary') THEN t.name + '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length AS varchar(10)) END + ')'
                    WHEN t.name IN ('nvarchar','nchar') THEN t.name + '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length / 2 AS varchar(10)) END + ')'
                    WHEN t.name IN ('decimal','numeric') THEN t.name + '(' + CAST(c.precision AS varchar(10)) + ',' + CAST(c.scale AS varchar(10)) + ')'
                    ELSE t.name
                END AS data_type,
                CAST(c.is_nullable AS bit) AS is_nullable,
                CASE
                    WHEN cc.object_id IS NOT NULL THEN CAST(cc.definition AS nvarchar(4000))
                    ELSE CAST(dc.definition AS nvarchar(4000))
                END AS default_expression,
                CASE
                    WHEN c.is_identity = 1 THEN 'identity'
                    WHEN c.is_computed = 1 THEN 'computed'
                END AS generated,
                CAST(ep.value AS nvarchar(4000)) AS description
            FROM 
                sys.columns c
                JOIN sys.objects o ON o.object_id = c.object_id AND o.type IN ('U','V')
                JOIN sys.schemas s ON s.schema_id = o.schema_id
                JOIN sys.types t ON t.user_type_id = c.user_type_id
                LEFT JOIN sys.computed_columns cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id    
                LEFT JOIN sys.default_constraints dc ON dc.object_id = c.default_object_id
                LEFT JOIN sys.extended_properties ep ON ep.class = 1 AND ep.major_id = c.object_id AND ep.minor_id = c.column_id AND ep.name = 'MS_Description'
            WHERE 
                s.name = {SCHEMA}
            ORDER BY 
                o.name, 
                c.column_id;
        """

    def _sql_primary_keys(self) -> str:
        return r"""
            SELECT
                t.name AS table_name,
                kc.name AS constraint_name,
                c.name AS column_name,
                ic.key_ordinal AS position
            FROM 
                sys.key_constraints kc
                JOIN sys.tables t ON t.object_id = kc.parent_object_id
                JOIN sys.schemas s ON s.schema_id = t.schema_id
                JOIN sys.index_columns ic ON ic.object_id = kc.parent_object_id AND ic.index_id = kc.unique_index_id AND ic.is_included_column = 0
                JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE 
                s.name = {SCHEMA}
                AND kc.type = 'PK'
            ORDER BY 
                t.name, 
                kc.name, 
                ic.key_ordinal;
        """

    def _sql_uniques(self) -> str:
        return r"""
            SELECT
                t.name AS table_name,
                kc.name AS constraint_name,
                c.name AS column_name,
                ic.key_ordinal AS position
            FROM 
                sys.key_constraints kc
                JOIN sys.tables t ON t.object_id = kc.parent_object_id
                JOIN sys.schemas s ON s.schema_id = t.schema_id
                JOIN sys.index_columns ic ON ic.object_id = kc.parent_object_id AND ic.index_id = kc.unique_index_id AND ic.is_included_column = 0
                JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE 
                s.name = {SCHEMA}
                AND kc.type = 'UQ'
            ORDER BY 
                t.name, 
                kc.name, 
                ic.key_ordinal;
        """

    def _sql_checks(self) -> str:
        return r"""
            SELECT
                t.name AS table_name,
                cc.name AS constraint_name,
                CAST(cc.definition AS nvarchar(4000)) AS expression,
                CAST(CASE 
                         WHEN cc.is_not_trusted = 0 THEN 1 
                         ELSE 0 
                    END AS bit) AS validated
            FROM 
                sys.check_constraints cc
                JOIN sys.tables t ON t.object_id = cc.parent_object_id
                JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE 
                s.name = {SCHEMA}
            ORDER BY 
                t.name, 
                cc.name;
        """

    def _sql_foreign_keys(self) -> str:
        return r"""
            SELECT
                t.name AS table_name,
                fk.name AS constraint_name,
                fkc.constraint_column_id AS position,
                pc.name AS from_column,
                rs.name AS ref_schema,
                rt.name AS ref_table,
                rc.name AS to_column,
                CAST(CASE WHEN fk.is_disabled = 0 THEN 1 ELSE 0 END AS bit) AS enforced,
                CAST(CASE WHEN fk.is_not_trusted = 0 THEN 1 ELSE 0 END AS bit) AS validated,
                LOWER(fk.update_referential_action_desc) AS on_update,
                LOWER(fk.delete_referential_action_desc) AS on_delete
            FROM 
                sys.foreign_keys fk
                JOIN sys.tables t   ON t.object_id = fk.parent_object_id
                JOIN sys.schemas s  ON s.schema_id = t.schema_id
                JOIN sys.tables rt  ON rt.object_id = fk.referenced_object_id
                JOIN sys.schemas rs ON rs.schema_id = rt.schema_id
                JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
                JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id    AND pc.column_id = fkc.parent_column_id
                JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
            WHERE 
                s.name = {SCHEMA}
            ORDER BY 
                t.name, 
                fk.name, 
                fkc.constraint_column_id;
        """

    # TODO: case when is confusing
    def _sql_indexes(self) -> str:
        return r"""
            SELECT
                t.name AS table_name,
                i.name AS index_name,
                ic.key_ordinal AS position,
                CAST(
                    CASE 
                        WHEN ic.is_descending_key = 1 THEN c.name + ' DESC'
                        ELSE c.name
                    END AS nvarchar(4000)
                ) AS expr,
                CAST(i.is_unique AS bit) AS is_unique,
                LOWER(i.type_desc) AS method,
                CAST(i.filter_definition AS nvarchar(4000)) AS predicate
            FROM 
                sys.indexes i
                JOIN sys.tables t   ON t.object_id = i.object_id
                JOIN sys.schemas s  ON s.schema_id = t.schema_id
                JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                JOIN sys.columns c   ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE 
                s.name = {SCHEMA}
                AND i.is_primary_key = 0
                AND i.is_unique_constraint = 0
                AND ic.is_included_column = 0
                AND ic.key_ordinal > 0
            ORDER BY 
                t.name, 
                i.name, 
                ic.key_ordinal;
        """

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

    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        with connection.cursor() as cursor:
            cursor.execute(sql, params or ())
            if not cursor.description:
                return []

            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def _quote_literal(self, value: str) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    def _sql_sample_rows(self, catalog: str, schema: str, table: str, limit: int) -> SQLQuery:
        sql = f'SELECT TOP ({limit}) * FROM "{catalog}"."{schema}"."{table}"'
        return SQLQuery(sql, [limit])
