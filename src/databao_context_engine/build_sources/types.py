from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path

from databao_context_engine.datasources.types import DatasourceId
from databao_context_engine.pluginlib.build_plugin import DatasourceType


@dataclass(frozen=True)
class OperationSummary:
    total: int
    ok: int
    skipped: int
    failed: int


class DatasourceStatus(Enum):
    OK = "ok"
    SKIPPED = "skipped"
    FAILED = "failed"


@dataclass(frozen=True)
class DatasourceResult:
    datasource_id: DatasourceId
    status: DatasourceStatus
    error: str | None = None


@dataclass(frozen=True)
class BuildDatasourceResult(DatasourceResult):
    datasource_type: DatasourceType | None = None
    context_built_at: datetime | None = None
    context_file_path: Path | None = None


@dataclass(frozen=True)
class IndexDatasourceResult(DatasourceResult):
    pass


@dataclass(frozen=True)
class BuildResult:
    summary: OperationSummary
    results: list[BuildDatasourceResult]


@dataclass(frozen=True)
class IndexResult:
    summary: OperationSummary
    results: list[IndexDatasourceResult]
