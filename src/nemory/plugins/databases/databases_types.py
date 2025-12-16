from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal

class DatasetKind(str, Enum):
    TABLE = "table"
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized_view"
    EXTERNAL_TABLE = "external_table"


@dataclass
class CheckConstraint:
    name: str | None
    expression: str
    validated: bool | None


@dataclass
class KeyConstraint:
    name: str | None
    columns: list[str]
    validated: bool | None


@dataclass
class ForeignKeyColumnMap:
    from_column: str
    to_column: str


@dataclass
class ForeignKey:
    name: str | None
    mapping: list[ForeignKeyColumnMap]
    referenced_table: str
    enforced: bool | None = None
    validated: bool | None = None
    on_update: str | None = None
    on_delete: str | None = None
    cardinality_inferred: Literal["one_to_one", "many_to_one"] | None = None


@dataclass
class Index:
    name: str
    columns: list[str]
    unique: bool = False
    method: str | None = None
    predicate: str | None = None


@dataclass
class ColumnProfile:
    profiled_at: str | None
    method: Literal["engine_stats", "sampled", "full_scan"] | None = None
    null_fraction: float | None = None
    ndv_estimate: int | None = None
    min: str | None = None
    max: str | None = None
    avg_length: float | None = None
    max_length: int | None = None
    top_values: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class TableProfile:
    profiled_at: str | None = None
    method: Literal["engine_stats", "sampled", "full_scan"] | None = None
    row_count_estimate: int | None = None


@dataclass
class DatabaseColumn:
    name: str
    type: str
    nullable: bool
    description: str | None = None
    default_expression: str | None = None
    generated: Literal["identity", "computed"] | None = None
    checks: list[CheckConstraint] = field(default_factory=list)
    profile: ColumnProfile | None = None


@dataclass
class DatabasePartitionInfo:
    meta: dict[str, Any]
    partition_tables: list[str]


@dataclass
class DatabaseTable:
    name: str
    columns: list[DatabaseColumn]
    samples: list[dict[str, Any]]
    partition_info: DatabasePartitionInfo | None = None
    description: str | None = None
    kind: DatasetKind = DatasetKind.TABLE
    primary_key: KeyConstraint | None = None
    unique_constraints: list[KeyConstraint] = field(default_factory=list)
    checks: list[CheckConstraint] = field(default_factory=list)
    indexes: list[Index] = field(default_factory=list)
    foreign_keys: list[ForeignKey] = field(default_factory=list)
    profile: TableProfile | None = None



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
