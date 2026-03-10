from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Callable


class ProgressKind(str, Enum):
    OPERATION_STARTED = "operation_started"
    OPERATION_FINISHED = "operation_finished"
    DATASOURCE_STARTED = "datasource_started"
    DATASOURCE_TOTAL_STEPS_SET = "datasource_total_steps_set"
    DATASOURCE_STEP_COMPLETED = "datasource_step_completed"
    DATASOURCE_CURRENT_STEP_PROGRESS = "datasource_current_step_progress"
    DATASOURCE_FINISHED = "datasource_finished"


@dataclass(frozen=True, slots=True)
class ProgressEvent:
    kind: ProgressKind

    operation: str | None = None
    operation_total: int | None = None

    datasource_id: str | None = None
    datasource_index: int | None = None
    datasource_total: int | None = None

    total_steps: int | None = None
    step_increment: int | None = None
    current_units_completed: int | None = None
    current_units_total: int | None = None

    status: str | None = None
    error: str | None = None


ProgressCallback = Callable[[ProgressEvent], None]


class ProgressEmitter:
    def __init__(self, cb: ProgressCallback | None):
        self._cb = cb

    def emit(self, event: ProgressEvent) -> None:
        if self._cb is not None:
            self._cb(event)

    def operation_started(self, *, operation: str, total: int) -> None:
        self.emit(
            ProgressEvent(
                kind=ProgressKind.OPERATION_STARTED,
                operation=operation,
                operation_total=total,
            )
        )

    def operation_finished(self, *, operation: str) -> None:
        self.emit(
            ProgressEvent(
                kind=ProgressKind.OPERATION_FINISHED,
                operation=operation,
            )
        )

    def datasource_started(self, *, datasource_id: str, index: int, total: int) -> None:
        self.emit(
            ProgressEvent(
                kind=ProgressKind.DATASOURCE_STARTED,
                datasource_id=datasource_id,
                datasource_index=index,
                datasource_total=total,
            )
        )

    def datasource_total_steps_set(self, *, datasource_id: str, total_steps: int) -> None:
        self.emit(
            ProgressEvent(
                kind=ProgressKind.DATASOURCE_TOTAL_STEPS_SET,
                datasource_id=datasource_id,
                total_steps=total_steps,
            )
        )

    def datasource_step_completed(self, *, datasource_id: str, step_count: int = 1) -> None:
        self.emit(
            ProgressEvent(
                kind=ProgressKind.DATASOURCE_STEP_COMPLETED,
                datasource_id=datasource_id,
                step_increment=step_count,
            )
        )

    def datasource_current_step_progress(
        self,
        *,
        datasource_id: str,
        completed_units: int,
        total_units: int,
    ) -> None:
        self.emit(
            ProgressEvent(
                kind=ProgressKind.DATASOURCE_CURRENT_STEP_PROGRESS,
                datasource_id=datasource_id,
                current_units_completed=completed_units,
                current_units_total=total_units,
            )
        )

    def datasource_finished(
        self,
        *,
        datasource_id: str,
        index: int,
        total: int,
        status: str,
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
            )
        )
