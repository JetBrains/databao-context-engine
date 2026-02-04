from dataclasses import dataclass
from enum import Enum


class SqlClass(Enum):
    READ_ONLY = "read_only"
    WRITE = "write"
    UNKNOWN = "unknown"


@dataclass
class SqlReadOnlyDecision:
    classification: SqlClass
    reason: str | None = None


def classify_sql(sql: str) -> SqlReadOnlyDecision:
    return SqlReadOnlyDecision(classification=SqlClass.READ_ONLY)


def is_read_only_sql(sql: str) -> bool:
    # todo add sql parsing and allowlist of tokens here
    return True
