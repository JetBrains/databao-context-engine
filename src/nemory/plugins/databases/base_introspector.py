from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Mapping, Sequence, Union

from nemory.plugins.databases.databases_types import (
    DatabaseCatalog,
    DatabaseIntrospectionResult,
    DatabaseSchema,
    DatabaseTable,
)

logger = logging.getLogger(__name__)


class BaseIntrospector[T](ABC):
    supports_catalogs: bool = True
    _IGNORED_SCHEMAS: set[str] = {"information_schema"}
    _SAMPLE_LIMIT: int = 5

    def check_connection(self, file_config: T) -> None:
        with self._connect(file_config) as connection:
            self._fetchall_dicts(connection, "SELECT 1 as test", None)

    def introspect_database(self, file_config: T) -> DatabaseIntrospectionResult:
        with self._connect(file_config) as root_conn:
            catalogs = self._get_catalogs_adapted(root_conn, file_config)

        introspected_catalogs: list[DatabaseCatalog] = []

        for catalog in catalogs:
            with self._connect_to_catalog(file_config, catalog) as connection:
                schemas_map = self._get_schemas(connection, [catalog], file_config)
                schemas_map = self._filter_schemas(schemas_map, file_config)
                schema_names = schemas_map.get(catalog, [])

                tables_by_schema: list[DatabaseSchema] = []
                for schema in schema_names:
                    tables = self.collect_schema_model(connection, catalog, schema) or []
                    if tables:
                        for table in tables:
                            table.samples = self._collect_samples_for_table(connection, catalog, schema, table.name)
                        tables_by_schema.append(DatabaseSchema(name=schema, tables=tables))
                if tables_by_schema:
                    introspected_catalogs.append(DatabaseCatalog(name=catalog, schemas=tables_by_schema))
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

    @abstractmethod
    def collect_schema_model(self, connection, catalog: str, schema: str) -> list[DatabaseTable] | None:
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
    def _connect(self, file_config: T):
        raise NotImplementedError

    @abstractmethod
    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        raise NotImplementedError

    @abstractmethod
    def _get_catalogs(self, connection, file_config: T) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def _connect_to_catalog(self, file_config: T, catalog: str):
        """Return a connection scoped to `catalog`. For engines that
        don’t need a new connection, return a connection with the
        session set/USE’d to that catalog."""

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
