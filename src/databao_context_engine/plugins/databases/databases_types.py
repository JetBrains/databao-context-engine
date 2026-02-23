from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal


class DatasetKind(str, Enum):
    TABLE = "table"
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized_view"
    EXTERNAL_TABLE = "external_table"

    @classmethod
    def from_raw(cls, raw: str | None) -> "DatasetKind":
        default = cls.TABLE
        if raw is None:
            return default
        if isinstance(raw, str):
            raw = raw.lower()
        try:
            return cls(raw)
        except Exception:
            return default


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
class ColumnStats:
    null_count: int | None = None
    non_null_count: int | None = None
    distinct_count: int | None = None
    min_value: Any | None = None
    max_value: Any | None = None
    top_values: list[tuple[Any, int]] | None = None  # (value, frequency) pairs


@dataclass
class DatabaseColumn:
    name: str
    type: str
    nullable: bool
    description: str | None = None
    default_expression: str | None = None
    generated: Literal["identity", "computed"] | None = None
    checks: list[CheckConstraint] = field(default_factory=list)
    stats: ColumnStats | None = None


@dataclass
class DatabasePartitionInfo:
    meta: dict[str, Any]
    partition_tables: list[str]


@dataclass
class TableStats:
    row_count: int | None = None
    approximate: bool = True


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
    stats: TableStats | None = None


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
