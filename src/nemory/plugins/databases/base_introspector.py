from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Mapping, Sequence, Union

from nemory.plugins.databases.databases_types import (
    DatabaseCatalog,
    DatabaseColumn,
    DatabaseIntrospectionResult,
    DatabasePartitionInfo,
    DatabaseSchema,
    DatabaseTable,
)

logger = logging.getLogger(__name__)


class BaseIntrospector[T](ABC):
    supports_catalogs: bool = True
    _IGNORED_SCHEMAS: set[str] = {"information_schema"}
    _SAMPLE_LIMIT: int = 5

    def introspect_database(self, file_config: T) -> DatabaseIntrospectionResult:
        connection = self._connect(file_config)
        with connection:
            catalogs = self._get_catalogs_adapted(connection, file_config)
            schemas_per_catalog = self._get_schemas(connection, catalogs, file_config)
            schemas_per_catalog = self._filter_schemas(schemas_per_catalog, file_config)

            introspected_catalogs: list[DatabaseCatalog] = []
            for catalog in catalogs:
                schemas: list[DatabaseSchema] = []
                for schema in schemas_per_catalog.get(catalog, []):
                    columns_per_table = self._collect_columns_for_schema(connection, catalog, schema)
                    partition_infos_per_table = self._collect_partitions_for_schema(connection, catalog, schema)

                    tables: list[DatabaseTable] = []
                    for table, columns in columns_per_table.items():
                        samples = self._collect_samples_for_table(connection, catalog, schema, table)
                        tables.append(
                            DatabaseTable(
                                name=table,
                                columns=columns,
                                samples=samples,
                                partition_info=partition_infos_per_table.get(table),
                            )
                        )
                    schemas.append(DatabaseSchema(name=schema, tables=tables))
                introspected_catalogs.append(DatabaseCatalog(name=catalog, schemas=schemas))

            return DatabaseIntrospectionResult(catalogs=introspected_catalogs)

    def _get_catalogs_adapted(self, connection, file_config: T) -> list[str]:
        if self.supports_catalogs:
            return self._get_catalogs(connection, file_config)
        return [self._resolve_pseudo_catalog_name(file_config)]

    def _get_schemas(self, connection: Any, catalogs: list[str], file_config: T) -> dict[str, list[str]]:
        sql_query = self._sql_list_schemas(catalogs if self.supports_catalogs else None)
        rows = self._fetchall_dicts(connection, sql_query.sql, sql_query.params)

        schemas_per_catalog: dict[str, list[str]] = defaultdict(list)
        for row in rows:
            catalog_name = row.get("catalog_name") or catalogs[0]
            schema_name = row.get("schema_name")
            if catalog_name and schema_name:
                schemas_per_catalog[catalog_name].append(schema_name)
            else:
                logger.warning(
                    "Skipping row with missing catalog or schema name: catalog=%s, schema=%s, row=%s",
                    catalog_name,
                    schema_name,
                    row,
                )
        return schemas_per_catalog

    def _sql_list_schemas(self, catalogs: list[str] | None) -> SQLQuery:
        if self.supports_catalogs:
            sql = "SELECT catalog_name, schema_name FROM information_schema.schemata WHERE catalog_name = ANY(%s)"
            return SQLQuery(sql, (catalogs,))
        else:
            sql = "SELECT schema_name FROM information_schema.schemata"
            return SQLQuery(sql, None)

    def _filter_schemas(
        self,
        schemas_per_catalog: dict[str, list[str]],
        file_config: T,
    ) -> dict[str, list[str]]:
        ignored = {s.lower() for s in (self._ignored_schemas() or [])}

        return {
            catalog: kept
            for catalog, schemas in schemas_per_catalog.items()
            if (kept := [s for s in schemas if s.lower() not in ignored])
        }

    def _collect_columns_for_schema(self, connection, catalog: str, schema: str):
        sql_query = self._sql_columns_for_schema(catalog, schema)
        rows = self._fetchall_dicts(connection, sql_query.sql, sql_query.params)
        columns: dict[str, list] = {}

        for row in rows:
            table_name = row.get("table_name")
            if not table_name:
                continue

            columns.setdefault(table_name, []).append(self._construct_column(row))

        return columns

    def _collect_partitions_for_schema(self, connection, catalog: str, schema: str):
        sql_query = self._sql_partitions_for_schema(catalog, schema)
        partitions: dict[str, DatabasePartitionInfo] = {}
        if not sql_query:
            return partitions

        rows = self._fetchall_dicts(connection, sql_query.sql, sql_query.params)

        for row in rows:
            table_name = row.get("table_name")
            if not table_name:
                continue
            partitions[table_name] = self._construct_partition_info(row)

        return partitions

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
    def _connect(self, file_config: T):
        raise NotImplementedError

    @abstractmethod
    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        raise NotImplementedError

    @abstractmethod
    def _get_catalogs(self, connection, file_config: T) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def _sql_columns_for_schema(self, catalog: str, schema: str) -> SQLQuery:
        raise NotImplementedError

    @abstractmethod
    def _construct_column(self, row: dict[str, Any]) -> DatabaseColumn:
        raise NotImplementedError

    def _sql_partitions_for_schema(self, catalog: str, schema: str) -> SQLQuery | None:
        return None

    def _construct_partition_info(self, row: dict[str, Any]) -> DatabasePartitionInfo:
        raise NotImplementedError

    def _sql_sample_rows(self, catalog: str, schema: str, table: str, limit: int) -> SQLQuery:
        raise NotImplementedError

    def _resolve_pseudo_catalog_name(self, file_config: T) -> str:
        return "default"

    def _ignored_schemas(self) -> set[str]:
        return self._IGNORED_SCHEMAS


@dataclass
class SQLQuery:
    sql: str
    params: ParamsType = None


ParamsType = Union[Mapping[str, Any], Sequence[Any], None]
