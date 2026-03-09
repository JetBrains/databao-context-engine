from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal


class DatasetKind(str, Enum):
    TABLE = "table"
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized_view"
    EXTERNAL_TABLE = "external_table"
    SNAPSHOT = "snapshot"
    CLONE = "clone"

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


@dataclass(frozen=True)
class CardinalityRange:
    min_value: int
    max_value: int | None  # exclusive upper bound; None = no upper bound

    def contains(self, count: int) -> bool:
        if self.max_value is None:
            return count >= self.min_value
        return self.min_value <= count < self.max_value


class CardinalityBucket(Enum):
    ZERO = CardinalityRange(0, 1)
    ONE = CardinalityRange(1, 2)
    VERY_LOW = CardinalityRange(2, 5)
    LOW = CardinalityRange(5, 10)
    LOW_MEDIUM = CardinalityRange(10, 20)
    MEDIUM = CardinalityRange(20, 50)
    MEDIUM_HIGH = CardinalityRange(50, 100)
    HIGH = CardinalityRange(100, 1000)
    VERY_HIGH = CardinalityRange(1000, None)
    UNKNOWN = "UNKNOWN"

    @classmethod
    def from_distinct_count(cls, distinct_count: int | None) -> "CardinalityBucket":
        if distinct_count is None or distinct_count < 0:
            return cls.UNKNOWN

        for bucket in cls:
            if bucket == cls.UNKNOWN:
                continue
            if bucket.value.contains(distinct_count):
                return bucket

        return cls.HIGH

    @property
    def label(self) -> str:
        if self is CardinalityBucket.UNKNOWN:
            return "unknown"

        lower_bound = self.value.min_value
        upper_bound = self.value.max_value

        if upper_bound is None:
            return f"{lower_bound}+"
        if upper_bound == lower_bound + 1:
            return str(lower_bound)
        return f"{lower_bound}-{upper_bound - 1}"


@dataclass
class ColumnStats:
    null_count: int | None = None
    non_null_count: int | None = None
    distinct_count: int | None = None
    cardinality_kind: CardinalityBucket | None = None
    min_value: Any | None = None
    max_value: Any | None = None
    top_values: list[tuple[Any, int]] | None = None  # (value, frequency) pairs
    total_row_count: int | None = None

    @property
    def proportion_null(self) -> float | None:
        if self.null_count is not None and self.total_row_count is not None and self.total_row_count > 0:
            return (self.null_count / self.total_row_count) * 100
        return None

    @property
    def proportion_distinct(self) -> float | None:
        if self.distinct_count is not None and self.total_row_count is not None and self.total_row_count > 0:
            return (self.distinct_count / self.total_row_count) * 100
        return None


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
