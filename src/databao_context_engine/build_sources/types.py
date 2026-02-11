from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path

from databao_context_engine.datasources.types import DatasourceId
from databao_context_engine.pluginlib.build_plugin import DatasourceType


@dataclass(frozen=True)
class OperationSummary:
    """Aggregate summary for an operation run across multiple datasources.

    Attributes:
        total: Total number of datasources attempted.
        ok: Number of datasources completed successfully.
        skipped: Number of datasources skipped.
        failed: Number of datasources that failed due to an error.
    """

    total: int
    ok: int
    skipped: int
    failed: int


class DatasourceStatus(Enum):
    """Status of an operation for a single datasource."""

    OK = "ok"
    SKIPPED = "skipped"
    FAILED = "failed"


@dataclass(frozen=True)
class DatasourceResult:
    """Base result for a single datasource attempt.

    This represents the outcome for a specific datasource in an operation.
    There is exactly one entry per attempted datasource.

    Attributes:
        datasource_id: Datasource identifier.
        status: The final status for this datasource.
        error: Optional error message when status is FAILED.
    """

    datasource_id: DatasourceId
    status: DatasourceStatus
    error: str | None = None


@dataclass(frozen=True)
class BuildDatasourceResult(DatasourceResult):
    """Build result for a single datasource.

    Extends DatasourceResult with build artifacts produced when a datasource builds successfully.

    Attributes:
        datasource_type: Datasource type used to build the context.
        context_built_at: Timestamp for when the context was built.
        context_file_path: Path to the exported built context file.
    """

    datasource_type: DatasourceType | None = None
    context_built_at: datetime | None = None
    context_file_path: Path | None = None


@dataclass(frozen=True)
class IndexDatasourceResult(DatasourceResult):
    """Index result for a single datasource."""

    pass


@dataclass(frozen=True)
class BuildResult:
    """Result of the build operation across multiple datasources.

    Contains a summary plus per-datasource results.

    Attributes:
        summary: Aggregate counts for the build run.
        results: Per-datasource results for the build run.
    """

    summary: OperationSummary
    results: list[BuildDatasourceResult]


@dataclass(frozen=True)
class IndexResult:
    """Result of the index operation across multiple datasources.

    Contains a summary plus per-datasource results.

    Attributes:
        summary: Aggregate counts for the index run.
        results: Per-datasource results for the index run.
    """

    summary: OperationSummary
    results: list[IndexDatasourceResult]
