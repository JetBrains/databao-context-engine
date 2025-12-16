from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Mapping, Sequence, Union

from nemory.plugins.databases.databases_types import (
    CheckConstraint,
    DatabaseCatalog,
    DatabaseColumn,
    DatabaseIntrospectionResult,
    DatabasePartitionInfo,
    DatabaseSchema,
    DatabaseTable,
    DatasetKind,
    ForeignKey,
    Index,
    KeyConstraint,
)

logger = logging.getLogger(__name__)


class BaseIntrospector[T](ABC):
    supports_catalogs: bool = True
    _IGNORED_SCHEMAS: set[str] = {"information_schema"}

    def check_connection(self, file_config: T) -> None:
        pass
        # with self._connect(file_config) as connection:
        #     self._fetchall_dicts(connection, "SELECT 1 as test", None)

    def introspect_database(self, file_config: T) -> DatabaseIntrospectionResult:
        with self._connect(file_config) as connection:
            catalogs = self._get_catalogs_adapted(connection, file_config)

        introspected_catalogs: list[DatabaseCatalog] = []

        for catalog in catalogs:
            catalog_config = self.config_for_catalog(file_config, catalog)
            with self._connect(catalog_config) as connection:
                raw_schemas = self.get_schemas(connection, catalog, file_config)
                schema_names = self._filter_schemas(raw_schemas)

                schemas: list[DatabaseSchema] = []
                for schema in schema_names:
                    try:
                        partition_infos_per_table = self.collect_partitions_for_schema(connection, catalog, schema)
                        kinds = self.collect_dataset_kinds(connection, catalog, schema)
                        columns_per_table = self.collect_columns(connection, catalog, schema)
                        primary_keys = self.collect_primary_keys(connection, catalog, schema)
                        unique_constraints = self.collect_unique_constraints(connection, catalog, schema)
                        table_checks = self.collect_table_checks(connection, catalog, schema)
                        foreign_keys = self.collect_foreign_keys(connection, catalog, schema)
                        indexes = self.collect_indexes(connection, catalog, schema)

                        tables = []
                        for table_name in sorted(columns_per_table.keys(), key=lambda s: (s.lower(), s)):
                            table = DatabaseTable(
                                name=table_name,
                                columns=columns_per_table[table_name],
                                samples=[],
                                kind=kinds.get(table_name, DatasetKind.TABLE),
                                primary_key=primary_keys.get(table_name, None),
                                unique_constraints=unique_constraints.get(table_name, []),
                                checks=table_checks.get(table_name, []),
                                foreign_keys=foreign_keys.get(table_name, []),
                                indexes=indexes.get(table_name, []),
                                partition_info=partition_infos_per_table.get(table_name),
                            )
                            tables.append(table)

                        schemas.append(DatabaseSchema(name=schema, tables=tables))
                    except Exception as e:
                        raise e
                introspected_catalogs.append(DatabaseCatalog(name=catalog, schemas=schemas))

        return DatabaseIntrospectionResult(catalogs=introspected_catalogs)

    def _get_catalogs_adapted(self, connection, file_config: T) -> list[str]:
        if self.supports_catalogs:
            return self._get_catalogs(connection, file_config)
        return [self._resolve_pseudo_catalog_name(file_config)]

    def _filter_schemas(self, schemas: list[str]) -> list[str]:
        ignored = {s.lower() for s in (self._ignored_schemas() or [])}
        return [s for s in schemas if s.lower() not in ignored]

    def qualify(self, catalog: str, schema: str, table: str) -> str:
        """
        Produce a stable qualified name for cross-references
        """
        if self.supports_catalogs:
            return f"{catalog}.{schema}.{table}"
        return f"{schema}.{table}"

    @abstractmethod
    def _connect(self, file_config: T):
        raise NotImplementedError

    @abstractmethod
    def _get_catalogs(self, connection, file_config: T) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def get_schemas(self, connection, catalog: str, file_config: T) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def collect_dataset_kinds(self, connection, catalog: str, schema: str) -> dict[str, DatasetKind]:
        raise NotImplementedError

    @abstractmethod
    def collect_columns(self, connection, catalog: str, schema: str) -> dict[str, list[DatabaseColumn]]:
        raise NotImplementedError

    @abstractmethod
    def collect_primary_keys(self, connection, catalog: str, schema: str) -> dict[str, KeyConstraint | None]:
        raise NotImplementedError

    @abstractmethod
    def collect_unique_constraints(self, connection, catalog: str, schema: str) -> dict[str, list[KeyConstraint]]:
        raise NotImplementedError

    @abstractmethod
    def collect_table_checks(self, connection, catalog: str, schema: str) -> dict[str, list[CheckConstraint]]:
        raise NotImplementedError

    @abstractmethod
    def collect_foreign_keys(self, connection, catalog: str, schema: str) -> dict[str, list[ForeignKey]]:
        raise NotImplementedError

    @abstractmethod
    def collect_indexes(self, connection, catalog: str, schema: str) -> dict[str, list[Index]]:
        raise NotImplementedError

    def collect_partitions_for_schema(self, connection, catalog: str, schema: str) -> dict[str, DatabasePartitionInfo]:
        return {}

    def _construct_partition_info(self, row: dict[str, Any]) -> DatabasePartitionInfo:
        raise NotImplementedError

    def _resolve_pseudo_catalog_name(self, file_config: T) -> str:
        return "default"

    def _ignored_schemas(self) -> set[str]:
        return self._IGNORED_SCHEMAS

    def config_for_catalog(self, file_config: T, catalog: str) -> T:
        """
        Return a connection config bound to the given catalog (database).
        Engines that don't need per-catalog connections can return file_config unchanged.
        """
        return file_config


@dataclass
class SQLQuery:
    sql: str
    params: ParamsType = None


ParamsType = Union[Mapping[str, Any], Sequence[Any], None]
