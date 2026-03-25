from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Generic, Iterable, Mapping, Protocol, Sequence, TypeVar, Union

import databao_context_engine.perf.core as perf
from databao_context_engine.pluginlib.sql.sql_types import SqlExecutionResult
from databao_context_engine.plugins.databases.databases_types import (
    CardinalityBucket,
    CatalogScope,
    ColumnRef,
    ColumnStatsEntry,
    DatabaseCatalog,
    DatabaseIntrospectionResult,
    DatabaseSchema,
    SchemaRef,
    TableRef,
    TableStatsEntry,
)
from databao_context_engine.plugins.databases.introspection_model_builder import IntrospectionModelBuilder
from databao_context_engine.plugins.databases.introspection_scope import IntrospectionScope
from databao_context_engine.plugins.databases.introspection_scope_matcher import (
    IntrospectionScopeMatcher,
)
from databao_context_engine.plugins.databases.profiling_config import ProfilingConfig
from databao_context_engine.plugins.databases.sampling_scope import SamplingConfig
from databao_context_engine.plugins.databases.sampling_scope_matcher import SamplingScopeMatcher

logger = logging.getLogger(__name__)


class SupportsIntrospectionScope(Protocol):
    introspection_scope: IntrospectionScope | None


class SupportsSamplingScope(Protocol):
    sampling: SamplingConfig | None


class SupportsProfilingScope(Protocol):
    profiling: ProfilingConfig | None


class SupportsDatabaseScopes(SupportsIntrospectionScope, SupportsSamplingScope, SupportsProfilingScope, Protocol):
    """Marker protocol for configs usable with BaseIntrospector."""


T = TypeVar("T", bound="SupportsDatabaseScopes")


class BaseIntrospector(Generic[T], ABC):
    supports_catalogs: bool = True
    _IGNORED_SCHEMAS: set[str] = {"information_schema"}
    _SAMPLE_LIMIT: int = 5
    _LOW_CARDINALITY_THRESHOLD = 20

    def check_connection(self, file_config: T) -> None:
        with self._connect(file_config) as connection:
            self._fetchall_dicts(connection, self._connection_check_sql_query(), None)

    def _connection_check_sql_query(self) -> str:
        return "SELECT 1 as test"

    @perf.perf_span("db.introspect_database")
    def introspect_database(self, file_config: T) -> DatabaseIntrospectionResult:
        sampling_matcher = SamplingScopeMatcher(file_config.sampling, ignored_schemas=self._ignored_schemas())
        profiling_enabled = bool(file_config.profiling and file_config.profiling.enabled)
        scope_matcher = IntrospectionScopeMatcher(
            file_config.introspection_scope,
            ignored_schemas=self._ignored_schemas(),
        )

        with self._connect(file_config) as root_connection:
            catalogs = self._get_catalogs_adapted(root_connection, file_config)

        introspected_catalogs: list[DatabaseCatalog] = []
        for catalog in catalogs:
            with self._connect(file_config, catalog=catalog) as conn:
                all_schemas = self._list_schemas_for_catalog(conn, catalog)
                schemas_to_introspect = scope_matcher.filter_schemas_for_catalog(catalog, all_schemas)

                if not schemas_to_introspect:
                    continue

                schemas = self._collect_catalog_model_timed(
                    connection=conn, catalog=catalog, schemas=schemas_to_introspect
                )
                if not schemas:
                    continue

                self._collect_samples_for_schemas(
                    connection=conn, catalog=catalog, schemas=schemas, sampling_matcher=sampling_matcher
                )

                if profiling_enabled:
                    self._collect_statistics_for_schemas(connection=conn, catalog=catalog, schemas=schemas)

                introspected_catalogs.append(DatabaseCatalog(name=catalog, schemas=schemas))

        return DatabaseIntrospectionResult(catalogs=introspected_catalogs)

    @perf.perf_span("db.collect_samples", attrs=lambda self, *, catalog, **_: {"catalog": catalog})
    def _collect_samples_for_schemas(
        self,
        *,
        connection: Any,
        catalog: str,
        schemas: list[DatabaseSchema],
        sampling_matcher: SamplingScopeMatcher,
    ) -> None:
        for schema in schemas:
            for table in schema.tables:
                if sampling_matcher.should_sample(catalog, schema.name, table.name):
                    collected_table_samples = self._collect_samples_for_table(
                        connection, catalog, schema.name, table.name
                    )

                    normalized_samples = []
                    for sample in collected_table_samples:
                        normalized_samples.append(
                            {
                                column_key: self._normalize_sample_value(sample_value)
                                for column_key, sample_value in sample.items()
                            }
                        )
                    table.samples = normalized_samples

    @perf.perf_span(
        "db.collect_catalog_model",
        attrs=lambda self, *, catalog, schemas, **_: {"catalog": catalog, "schema_count": len(schemas)},
    )
    def _collect_catalog_model_timed(
        self, *, connection: Any, catalog: str, schemas: list[str]
    ) -> list[DatabaseSchema] | None:
        return self.collect_catalog_model(connection, catalog, schemas)

    @perf.perf_span("db.collect_statistics", attrs=lambda self, *, catalog, **_: {"catalog": catalog})
    def _collect_statistics_for_schemas(self, *, connection: Any, catalog: str, schemas: list[DatabaseSchema]) -> None:
        scope = self._build_catalog_scope(catalog, schemas)
        table_stats, column_stats = self.collect_stats(connection, catalog, scope)

        if table_stats:
            table_stats_map = {(e.schema_name, e.table_name): e for e in table_stats}
            for schema in schemas:
                for table in schema.tables:
                    entry = table_stats_map.get((schema.name, table.name))
                    if entry:
                        table.stats = entry.stats

        if column_stats:
            column_stats_map = {(e.schema_name, e.table_name, e.column_name): e for e in column_stats}
            for schema in schemas:
                for table in schema.tables:
                    table_row_count = table.stats.row_count if table.stats else None
                    for column in table.columns:
                        entry = column_stats_map.get((schema.name, table.name, column.name))
                        if entry:
                            if entry.stats.total_row_count is None:
                                entry.stats.total_row_count = table_row_count
                            column.stats = entry.stats

    def _build_catalog_scope(self, catalog: str, schemas: list[DatabaseSchema]) -> CatalogScope:
        return CatalogScope(
            catalog_name=catalog,
            schemas=[
                SchemaRef(
                    schema_name=schema.name,
                    tables=[
                        TableRef(
                            table_name=table.name,
                            kind=table.kind,
                            columns=[ColumnRef(name=col.name, type=col.type) for col in table.columns],
                        )
                        for table in schema.tables
                    ],
                )
                for schema in schemas
            ],
        )

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

    def collect_catalog_model(self, connection: Any, catalog: str, schemas: list[str]) -> list[DatabaseSchema] | None:
        if not schemas:
            return []

        relations = self.collect_relations(connection, catalog, schemas)
        table_columns = self.collect_table_columns(connection, catalog, schemas)
        view_columns = self.collect_view_columns(connection, catalog, schemas) or []
        pk = self.collect_primary_keys(connection, catalog, schemas) or []
        uq = self.collect_unique_constraints(connection, catalog, schemas) or []
        checks = self.collect_checks(connection, catalog, schemas) or []
        fks = self.collect_foreign_keys(connection, catalog, schemas) or []
        idx = self.collect_indexes(connection, catalog, schemas) or []
        partitions = self.collect_partitions(connection, catalog, schemas) or []

        columns = table_columns + view_columns

        return IntrospectionModelBuilder.build_schemas_from_components(
            schemas=schemas,
            rels=relations,
            cols=columns,
            pk_cols=pk,
            uq_cols=uq,
            checks=checks,
            fk_cols=fks,
            idx_cols=idx,
            partitions=partitions,
        )

    def collect_relations(self, connection, catalog: str, schemas: list[str]) -> list[dict]:
        sql_query = self.get_relations_sql_query(catalog, schemas)

        return self._fetchall_dicts(connection, sql_query.sql, sql_query.params)

    @abstractmethod
    def get_relations_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        raise NotImplementedError

    def collect_table_columns(self, connection, catalog: str, schemas: list[str]) -> list[dict]:
        sql_query = self.get_table_columns_sql_query(catalog, schemas)

        return self._fetchall_dicts(connection, sql_query.sql, sql_query.params)

    @abstractmethod
    def get_table_columns_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery:
        raise NotImplementedError

    def collect_view_columns(self, connection, catalog: str, schemas: list[str]) -> list[dict] | None:
        sql_query = self.get_view_columns_sql_query(catalog, schemas)

        if sql_query is not None:
            try:
                return self._fetchall_dicts(connection, sql_query.sql, sql_query.params)
            except Exception:
                # FIXME: We need a way for plugins to report non-critical errors happening during the build
                logger.debug("Error while fetching view columns", exc_info=True, stack_info=True)
                return None

        return None

    def get_view_columns_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery | None:
        return None

    def collect_primary_keys(self, connection, catalog: str, schemas: list[str]) -> list[dict] | None:
        sql_query = self.get_primary_keys_sql_query(catalog, schemas)
        if sql_query is not None:
            return self._fetchall_dicts(connection, sql_query.sql, sql_query.params)

        return None

    def get_primary_keys_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery | None:
        return None

    def collect_unique_constraints(self, connection, catalog: str, schemas: list[str]) -> list[dict] | None:
        sql_query = self.get_unique_constraints_sql_query(catalog, schemas)
        if sql_query is not None:
            return self._fetchall_dicts(connection, sql_query.sql, sql_query.params)

        return None

    def get_unique_constraints_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery | None:
        return None

    def collect_checks(self, connection, catalog: str, schemas: list[str]) -> list[dict] | None:
        sql_query = self.get_checks_sql_query(catalog, schemas)
        if sql_query is not None:
            return self._fetchall_dicts(connection, sql_query.sql, sql_query.params)

        return None

    def get_checks_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery | None:
        return None

    def collect_foreign_keys(self, connection, catalog: str, schemas: list[str]) -> list[dict] | None:
        sql_query = self.get_foreign_keys_sql_query(catalog, schemas)
        if sql_query is not None:
            return self._fetchall_dicts(connection, sql_query.sql, sql_query.params)

        return None

    def get_foreign_keys_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery | None:
        return None

    def collect_indexes(self, connection, catalog: str, schemas: list[str]) -> list[dict] | None:
        sql_query = self.get_indexes_sql_query(catalog, schemas)
        if sql_query is not None:
            return self._fetchall_dicts(connection, sql_query.sql, sql_query.params)

        return None

    def get_indexes_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery | None:
        return None

    def collect_partitions(self, connection, catalog: str, schemas: list[str]) -> list[dict] | None:
        sql_query = self.get_partitions_sql_query(catalog, schemas)
        if sql_query is not None:
            return self._fetchall_dicts(connection, sql_query.sql, sql_query.params)

        return None

    def get_partitions_sql_query(self, catalog: str, schemas: list[str]) -> SQLQuery | None:
        return None

    def collect_stats(
        self,
        connection,
        catalog: str,
        scope: CatalogScope,
    ) -> tuple[list[TableStatsEntry] | None, list[ColumnStatsEntry] | None]:
        return None, None

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

    @staticmethod
    def _compute_cardinality_stats(
        distinct_count: int | None,
    ) -> tuple[CardinalityBucket, int | None]:
        cardinality_kind = CardinalityBucket.from_distinct_count(distinct_count)
        low_cardinality_distinct_count = (
            distinct_count
            if distinct_count is not None and distinct_count < BaseIntrospector._LOW_CARDINALITY_THRESHOLD
            else None
        )
        return cardinality_kind, low_cardinality_distinct_count

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

    def _normalize_sample_value(self, sample_value: Any) -> Any:
        """Normalize complex sample values to a string.

        It also truncates string that are too long to prevent bloating the samples with unnecessary long values.

        Returns:
            The normalized (and truncated if necessary) sample value
        """
        if isinstance(sample_value, bytes | bytearray):
            return f"<bytes, {len(sample_value)} bytes>"

        if isinstance(sample_value, str):
            return self._truncate_sample_string(sample_value)

        if isinstance(sample_value, Iterable | Mapping):
            return self._normalize_sample_value(json.dumps(sample_value, default=str))

        return sample_value

    _SAMPLE_VALUE_SIZE_LIMIT = 256

    def _truncate_sample_string(self, sample_value: str) -> str:
        if len(sample_value) > self._SAMPLE_VALUE_SIZE_LIMIT:
            return f"{sample_value[: self._SAMPLE_VALUE_SIZE_LIMIT]}…[truncated, {self._SAMPLE_VALUE_SIZE_LIMIT}/{len(sample_value)}]"

        return sample_value


@dataclass
class SQLQuery:
    sql: str
    params: ParamsType = None


ParamsType = Union[Mapping[str, Any], Sequence[Any], None]
