from dataclasses import dataclass
from typing import Any


@dataclass
class DatabaseColumn:
    name: str
    type: str
    nullable: bool
    description: str | None = None


@dataclass
class DatabaseTable:
    name: str
    columns: list[DatabaseColumn]
    samples: list[dict[str, Any]]
    description: str | None = None


@dataclass
class DatabaseSchema:
    name: str
    tables: list[DatabaseTable]
    description: str | None = None


@dataclass
class DatabaseCatalog:
    name: str
    schemas: list[DatabaseSchema]
    description: str | None = None


@dataclass
class DatabaseIntrospectionResult:
    catalogs: list[DatabaseCatalog]
