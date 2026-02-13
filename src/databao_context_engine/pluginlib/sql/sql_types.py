from dataclasses import dataclass
from typing import Any


@dataclass
class SqlExecutionResult:
    columns: list[str]
    rows: list[tuple[Any, ...]]
