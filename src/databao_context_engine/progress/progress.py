from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Callable


class ProgressKind(str, Enum):
    # Build-level
    BUILD_STARTED = "build_started"
    BUILD_FINISHED = "build_finished"

    # Datasource-level lifecycle
    DATASOURCE_STARTED = "datasource_started"
    DATASOURCE_FINISHED = "datasource_finished"

    # Datasource-level detail / phases
    DATASOURCE_PHASE = "datasource_phase"
    CHUNKS_DISCOVERED = "chunks_discovered"

    # Chunk embedding
    EMBEDDING_STARTED = "embedding_started"
    EMBEDDING_PROGRESS = "embedding_progress"
    EMBEDDING_FINISHED = "embedding_finished"

    # Persistence
    PERSIST_STARTED = "persist_started"
    PERSIST_PROGRESS = "persist_progress"
    PERSIST_FINISHED = "persist_finished"


class DatasourceStatus(str, Enum):
    OK = "ok"
    SKIPPED = "skipped"
    FAILED = "failed"


class DatasourcePhase(str, Enum):
    EXECUTE_PLUGIN = "execute_plugin"
    DIVIDE_CHUNKS = "divide_chunks"
    EMBED = "embed"
    PERSIST = "persist"
    EXPORT = "export"


@dataclass(frozen=True, slots=True)
class ProgressEvent:
    """A structured progress update emitted by the build pipeline.

    Conventions:
    - `total` may be None when unknown.
    - For *_PROGRESS events, `done` should be monotonic increasing up to `total` (if total known).
    - `message` is optional human-readable text; callers can ignore it and render their own.
    """

    kind: ProgressKind

    # Identifiers (often present, but not required for build-level events)
    datasource_id: str | None = None
    datasource_type: str | None = None  # e.g. full_type
    datasource_path: str | None = None  # if helpful for debugging/logging

    # Build progress (datasources)
    datasource_index: int | None = None     # 1-based index
    datasource_total: int | None = None     # total datasources

    # Sub-progress (chunks / persistence)
    done: int | None = None
    total: int | None = None

    # Extra structured fields
    phase: DatasourcePhase | None = None
    status: DatasourceStatus | None = None
    error: str | None = None

    # Human text
    message: str = ""


ProgressCallback = Callable[[ProgressEvent], None]


class ProgressEmitter:
    """Small helper so you don't sprinkle `if progress: progress(...)` everywhere.

    You can also extend this later with throttling helpers.
    """

    def __init__(self, cb: ProgressCallback | None):
        self._cb = cb

    def emit(self, event: ProgressEvent) -> None:
        if self._cb is not None:
            self._cb(event)

    # Convenience builders (optional, but nice for consistency)
    def build_started(self, *, total_datasources: int | None) -> None:
        self.emit(ProgressEvent(kind=ProgressKind.BUILD_STARTED, datasource_total=total_datasources))

    def build_finished(self, *, ok: int, failed: int, skipped: int) -> None:
        self.emit(
            ProgressEvent(
                kind=ProgressKind.BUILD_FINISHED,
                message=f"Finished (ok={ok}, failed={failed}, skipped={skipped})",
            )
        )

    def datasource_started(
        self,
        *,
        datasource_id: str,
        datasource_type: str | None,
        datasource_path: str | None,
        index: int,
        total: int,
    ) -> None:
        self.emit(
            ProgressEvent(
                kind=ProgressKind.DATASOURCE_STARTED,
                datasource_id=datasource_id,
                datasource_type=datasource_type,
                datasource_path=datasource_path,
                datasource_index=index,
                datasource_total=total,
                message=f"Starting {datasource_id}",
            )
        )

    def datasource_phase(self, *, datasource_id: str, phase: DatasourcePhase, message: str = "") -> None:
        self.emit(
            ProgressEvent(
                kind=ProgressKind.DATASOURCE_PHASE,
                datasource_id=datasource_id,
                phase=phase,
                message=message or phase.value,
            )
        )

    def chunks_discovered(self, *, datasource_id: str, total_chunks: int) -> None:
        self.emit(
            ProgressEvent(
                kind=ProgressKind.CHUNKS_DISCOVERED,
                datasource_id=datasource_id,
                total=total_chunks,
                message=f"Discovered {total_chunks} chunks",
            )
        )

    def embedding_started(self, *, datasource_id: str, total_chunks: int) -> None:
        self.emit(
            ProgressEvent(
                kind=ProgressKind.EMBEDDING_STARTED,
                datasource_id=datasource_id,
                total=total_chunks,
                message="Embedding started",
            )
        )

    def embedding_progress(self, *, datasource_id: str, done: int, total: int | None, message: str = "") -> None:
        self.emit(
            ProgressEvent(
                kind=ProgressKind.EMBEDDING_PROGRESS,
                datasource_id=datasource_id,
                done=done,
                total=total,
                message=message,
            )
        )

    def embedding_finished(self, *, datasource_id: str) -> None:
        self.emit(ProgressEvent(kind=ProgressKind.EMBEDDING_FINISHED, datasource_id=datasource_id, message="Embedding finished"))

    def persist_started(self, *, datasource_id: str, total_items: int) -> None:
        self.emit(
            ProgressEvent(
                kind=ProgressKind.PERSIST_STARTED,
                datasource_id=datasource_id,
                total=total_items,
                message="Persist started",
            )
        )

    def persist_progress(self, *, datasource_id: str, done: int, total: int | None, message: str = "") -> None:
        self.emit(
            ProgressEvent(
                kind=ProgressKind.PERSIST_PROGRESS,
                datasource_id=datasource_id,
                done=done,
                total=total,
                message=message,
            )
        )

    def persist_finished(self, *, datasource_id: str) -> None:
        self.emit(ProgressEvent(kind=ProgressKind.PERSIST_FINISHED, datasource_id=datasource_id, message="Persist finished"))

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
                message=(f"Finished {datasource_id}" if status == DatasourceStatus.OK else f"{status.value}: {datasource_id}"),
            )
        )