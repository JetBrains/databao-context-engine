from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Callable

EMIT_EVERY = 10


class ProgressKind(str, Enum):
    TASK_STARTED = "task_started"
    TASK_FINISHED = "task_finished"
    DATASOURCE_STARTED = "datasource_started"
    DATASOURCE_FINISHED = "datasource_finished"
    DATASOURCE_PROGRESS = "datasource_progress"


class DatasourceStatus(str, Enum):
    OK = "ok"
    SKIPPED = "skipped"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class ProgressEvent:
    kind: ProgressKind
    datasource_id: str | None = None
    datasource_index: int | None = None
    datasource_total: int | None = None
    percent: int | None = None
    status: DatasourceStatus | None = None
    error: str | None = None
    message: str = ""


ProgressCallback = Callable[[ProgressEvent], None]


class ProgressEmitter:
    def __init__(self, cb: ProgressCallback | None):
        self._cb = cb

    def emit(self, event: ProgressEvent) -> None:
        if self._cb is not None:
            self._cb(event)

    def task_started(self, *, total_datasources: int | None) -> None:
        self.emit(ProgressEvent(kind=ProgressKind.TASK_STARTED, datasource_total=total_datasources))

    def task_finished(self, *, ok: int, failed: int, skipped: int) -> None:
        self.emit(
            ProgressEvent(
                kind=ProgressKind.TASK_FINISHED,
                message=f"Finished (ok={ok}, failed={failed}, skipped={skipped})",
            )
        )

    def datasource_started(
        self,
        *,
        datasource_id: str,
        index: int,
        total: int,
    ) -> None:
        self.emit(
            ProgressEvent(
                kind=ProgressKind.DATASOURCE_STARTED,
                datasource_id=datasource_id,
                datasource_index=index,
                datasource_total=total,
                message=f"Starting {datasource_id}",
            )
        )

    def datasource_progress_units(
        self,
        *,
        datasource_id: str,
        completed_units: int,
        total_units: int,
        message: str = "",
    ) -> None:
        if total_units <= 0:
            self.datasource_progress(datasource_id=datasource_id, percent=100, message=message)
            return

        completed_units = max(0, min(completed_units, total_units))
        percent = round((completed_units / total_units) * 100)
        self.datasource_progress(datasource_id=datasource_id, percent=percent, message=message)

    def datasource_progress(self, *, datasource_id: str, percent: int, message: str = "") -> None:
        if percent < 0:
            percent = 0
        if percent > 100:
            percent = 100
        self.emit(
            ProgressEvent(
                kind=ProgressKind.DATASOURCE_PROGRESS,
                datasource_id=datasource_id,
                percent=percent,
                message=message,
            )
        )

    def datasource_finished(
        self,
        *,
        datasource_id: str,
        index: int,
        total: int,
        status: DatasourceStatus,
        error: str | None = None,
    ) -> None:
        self.emit(
            ProgressEvent(
                kind=ProgressKind.DATASOURCE_FINISHED,
                datasource_id=datasource_id,
                datasource_index=index,
                datasource_total=total,
                status=status,
                error=error,
                message=(
                    f"Finished {datasource_id}" if status == DatasourceStatus.OK else f"{status.value}: {datasource_id}"
                ),
            )
        )
