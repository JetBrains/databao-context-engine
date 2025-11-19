from __future__ import annotations

import logging
from abc import abstractmethod, ABC
from collections import defaultdict
from typing import Any, Mapping

from nemory.plugins.databases.databases_types import (
    DatabaseCatalog,
    DatabaseIntrospectionResult,
    DatabaseSchema,
    DatabaseTable,
    DatabaseColumn,
)

logger = logging.getLogger(__name__)


class BaseIntrospector(ABC):
    supports_catalogs: bool = True
    _IGNORED_SCHEMAS: set[str] = {"information_schema"}

    def introspect_database(self, file_config: Mapping[str, Any]) -> DatabaseIntrospectionResult:
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
                    schemas.append(
                        DatabaseSchema(
                            name=schema,
                            tables=[
                                DatabaseTable(name=table, columns=columns_per_table[table], samples=[])
                                for table in columns_per_table
                            ],
                        )
                    )
                introspected_catalogs.append(DatabaseCatalog(name=catalog, schemas=schemas))

            return DatabaseIntrospectionResult(catalogs=introspected_catalogs)

    def _get_catalogs_adapted(self, connection, file_config: Mapping[str, Any]) -> list[str]:
        if self.supports_catalogs:
            return self._get_catalogs(connection, file_config)
        return [self._resolve_pseudo_catalog_name(file_config)]

    def _get_schemas(
        self, connection: Any, catalogs: list[str], file_config: Mapping[str, Any]
    ) -> dict[str, list[str]]:
        sql, params = self._sql_list_schemas(catalogs if self.supports_catalogs else None)
        rows = self._fetchall_dicts(connection, sql, params)

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

    def _sql_list_schemas(self, catalogs: list[str] | None) -> tuple[str, tuple | list | None]:
        if self.supports_catalogs:
            sql = "SELECT catalog_name, schema_name FROM information_schema.schemata WHERE catalog_name = ANY(%s) "
            return sql, (catalogs,)
        else:
            sql = "SELECT schema_name FROM information_schema.schemata "
            return sql, None

    def _filter_schemas(
        self,
        schemas_per_catalog: dict[str, list[str]],
        file_config: Mapping[str, Any],
    ) -> dict[str, list[str]]:
        ignored = {s.lower() for s in (self._ignored_schemas() or [])}

        return {
            catalog: kept
            for catalog, schemas in schemas_per_catalog.items()
            if (kept := [s for s in schemas if s.lower() not in ignored])
        }

    def _collect_columns_for_schema(self, connection, catalog: str, schema: str):
        sql, params = self._sql_columns_for_schema(catalog, schema)
        rows = self._fetchall_dicts(connection, sql, params)

        tables: dict[str, list] = {}

        for row in rows:
            table_name = row.get("table_name")
            if not table_name:
                continue

            tables.setdefault(table_name, []).append(self._construct_column(row))

        return tables

    @abstractmethod
    def _connect(self, file_config: Mapping[str, Any]):
        raise NotImplementedError

    @abstractmethod
    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        raise NotImplementedError

    @abstractmethod
    def _get_catalogs(self, connection, file_config: Mapping[str, Any]) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def _sql_columns_for_schema(self, catalog: str, schema: str) -> tuple[str, tuple | list]:
        raise NotImplementedError

    @abstractmethod
    def _construct_column(self, row: dict[str, Any]) -> DatabaseColumn:
        raise NotImplementedError

    def _resolve_pseudo_catalog_name(self, file_config: Mapping[str, Any]) -> str:
        return "default"

    def _ignored_schemas(self) -> set[str]:
        return self._IGNORED_SCHEMAS
