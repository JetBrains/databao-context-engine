from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Callable


class ProgressKind(str, Enum):
    OPERATION_STARTED = "operation_started"
    OPERATION_FINISHED = "operation_finished"
    DATASOURCE_STARTED = "datasource_started"
    DATASOURCE_STEP_PLAN_SET = "datasource_step_plan_set"
    DATASOURCE_STEP_PROGRESS = "datasource_step_progress"
    DATASOURCE_STEP_COMPLETED = "datasource_step_completed"
    DATASOURCE_FINISHED = "datasource_finished"


class ProgressStep(str, Enum):
    PLUGIN_EXECUTION = "plugin_execution"
    CONTEXT_ENRICHMENT = "context_enrichment"
    EMBEDDING = "embedding"
    PERSISTENCE = "persistence"


@dataclass(frozen=True, slots=True)
class ProgressEvent:
    kind: ProgressKind

    operation: str | None = None
    operation_total: int | None = None

    datasource_id: str | None = None
    datasource_index: int | None = None
    datasource_total: int | None = None

    step_plan: tuple[ProgressStep, ...] | None = None
    step: ProgressStep | None = None
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

    def datasource_step_plan_set(self, *, datasource_id: str, step_plan: tuple[ProgressStep, ...]) -> None:
        self.emit(
            ProgressEvent(
                kind=ProgressKind.DATASOURCE_STEP_PLAN_SET,
                datasource_id=datasource_id,
                step_plan=step_plan,
            )
        )

    def datasource_step_completed(self, *, datasource_id: str, step: ProgressStep) -> None:
        self.emit(
            ProgressEvent(
                kind=ProgressKind.DATASOURCE_STEP_COMPLETED,
                datasource_id=datasource_id,
                step=step,
            )
        )

    def datasource_step_progress(
        self,
        *,
        datasource_id: str,
        step: ProgressStep,
        completed_units: int,
        total_units: int,
    ) -> None:
        self.emit(
            ProgressEvent(
                kind=ProgressKind.DATASOURCE_STEP_PROGRESS,
                datasource_id=datasource_id,
                step=step,
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
