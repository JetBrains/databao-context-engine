from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Mapping, Protocol, Sequence, Union

import databao_context_engine.perf.core as perf
from databao_context_engine.pluginlib.sql.sql_types import SqlExecutionResult
from databao_context_engine.plugins.databases.databases_types import (
    DatabaseCatalog,
    DatabaseIntrospectionResult,
    DatabaseSchema,
)
from databao_context_engine.plugins.databases.introspection_scope import IntrospectionScope
from databao_context_engine.plugins.databases.introspection_scope_matcher import IntrospectionScopeMatcher

logger = logging.getLogger(__name__)


class SupportsIntrospectionScope(Protocol):
    introspection_scope: IntrospectionScope | None


class BaseIntrospector[T: SupportsIntrospectionScope](ABC):
    supports_catalogs: bool = True
    _IGNORED_SCHEMAS: set[str] = {"information_schema"}
    _SAMPLE_LIMIT: int = 5

    def check_connection(self, file_config: T) -> None:
        with self._connect(file_config) as connection:
            self._fetchall_dicts(connection, "SELECT 1 as test", None)

    @perf.perf_span("db.introspect_database")
    def introspect_database(self, file_config: T) -> DatabaseIntrospectionResult:
        scope_matcher = IntrospectionScopeMatcher(
            file_config.introspection_scope,
            ignored_schemas=self._ignored_schemas(),
        )

        with self._connect(file_config) as root_connection:
            catalogs = self._get_catalogs_adapted(root_connection, file_config)

        discovered_schemas_per_catalog: dict[str, list[str]] = {}
        for catalog in catalogs:
            with self._connect(file_config, catalog=catalog) as conn:
                discovered_schemas_per_catalog[catalog] = self._list_schemas_for_catalog(conn, catalog)
        scope = scope_matcher.filter_scopes(catalogs, discovered_schemas_per_catalog)

        introspected_catalogs: list[DatabaseCatalog] = []
        for catalog in scope.catalogs:
            schemas_to_introspect = scope.schemas_per_catalog.get(catalog, [])
            if not schemas_to_introspect:
                continue

            with self._connect(file_config, catalog=catalog) as catalog_connection:
                introspected_schemas = self._collect_catalog_model_timed(
                    connection=catalog_connection, catalog=catalog, schemas=schemas_to_introspect
                )

                if not introspected_schemas:
                    continue

                self._collect_samples_for_schemas_timed(
                    connection=catalog_connection, catalog=catalog, schemas=introspected_schemas
                )

                introspected_catalogs.append(DatabaseCatalog(name=catalog, schemas=introspected_schemas))
        return DatabaseIntrospectionResult(catalogs=introspected_catalogs)

    @perf.perf_span(
        "db.collect_catalog_model",
        attrs=lambda self, *, catalog, schemas, **_: {"catalog": catalog, "schema_count": len(schemas)},
    )
    def _collect_catalog_model_timed(
        self, *, connection: Any, catalog: str, schemas: list[str]
    ) -> list[DatabaseSchema] | None:
        return self.collect_catalog_model(connection, catalog, schemas)

    @perf.perf_span("db.collect_samples", attrs=lambda self, *, catalog, **_: {"catalog": catalog})
    def _collect_samples_for_schemas_timed(
        self, *, connection: Any, catalog: str, schemas: list[DatabaseSchema]
    ) -> None:
        for schema in schemas:
            for table in schema.tables:
                table.samples = self._collect_samples_for_table(connection, catalog, schema.name, table.name)

    def _get_catalogs_adapted(self, connection, file_config: T) -> list[str]:
        if self.supports_catalogs:
            return self._get_catalogs(connection, file_config)
        return [self._resolve_pseudo_catalog_name(file_config)]

    def _sql_list_schemas(self, catalogs: list[str] | None) -> SQLQuery:
        if self.supports_catalogs:
            sql = "SELECT catalog_name, schema_name FROM information_schema.schemata WHERE catalog_name = ANY(%s)"
            return SQLQuery(sql, (catalogs,))

        sql = "SELECT schema_name FROM information_schema.schemata"
        return SQLQuery(sql, None)

    def _list_schemas_for_catalog(self, connection: Any, catalog: str) -> list[str]:
        sql_query = self._sql_list_schemas([catalog] if self.supports_catalogs else None)
        rows = self._fetchall_dicts(connection, sql_query.sql, sql_query.params)

        schemas: list[str] = []
        for row in rows:
            schema_name = row.get("schema_name")
            if schema_name:
                schemas.append(schema_name)

        return schemas

    @abstractmethod
    def collect_catalog_model(self, connection, catalog: str, schemas: list[str]) -> list[DatabaseSchema] | None:
        raise NotImplementedError

    def _collect_samples_for_table(self, connection, catalog: str, schema: str, table: str) -> list[dict[str, Any]]:
        samples: list[dict[str, Any]] = []
        if self._SAMPLE_LIMIT > 0:
            try:
                sql_query = self._sql_sample_rows(catalog, schema, table, self._SAMPLE_LIMIT)
                samples = self._fetchall_dicts(connection, sql_query.sql, sql_query.params)
            except NotImplementedError:
                samples = []
            except Exception as e:
                logger.warning("Failed to fetch samples for %s.%s (catalog=%s): %s", schema, table, catalog, e)
                samples = []
        return samples

    @abstractmethod
    def _connect(self, file_config: T, *, catalog: str | None = None) -> Any:
        """Connect to the database.

        If the `catalog` argument is provided, the connection is "scoped" to that catalog. For engines that don’t need a new connection,
        return a connection with the session set/USE’d to that catalog.
        """
        raise NotImplementedError

    @abstractmethod
    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        raise NotImplementedError

    @abstractmethod
    def _get_catalogs(self, connection, file_config: T) -> list[str]:
        raise NotImplementedError

    def _sql_sample_rows(self, catalog: str, schema: str, table: str, limit: int) -> SQLQuery:
        raise NotImplementedError

    def _resolve_pseudo_catalog_name(self, file_config: T) -> str:
        return "default"

    def _ignored_schemas(self) -> set[str]:
        return self._IGNORED_SCHEMAS

    def run_sql(
        self,
        file_config: T,
        sql: str,
        params: list[Any] | None,
        read_only: bool,
    ) -> SqlExecutionResult:
        # for now, we don't have any read-only related logic implemented on the database side
        with self._connect(file_config) as connection:
            rows_dicts: list[dict] = self._fetchall_dicts(connection, sql, params)

        if not rows_dicts:
            return SqlExecutionResult(columns=[], rows=[])

        columns: list[str] = list(rows_dicts[0].keys())
        rows: list[tuple[Any, ...]] = [tuple(row.get(col) for col in columns) for row in rows_dicts]
        return SqlExecutionResult(columns=columns, rows=rows)


@dataclass
class SQLQuery:
    sql: str
    params: ParamsType = None


ParamsType = Union[Mapping[str, Any], Sequence[Any], None]
