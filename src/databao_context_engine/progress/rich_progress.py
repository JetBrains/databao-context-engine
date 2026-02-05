# databao_context_engine/cli/rich_progress.py
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
)

from databao_context_engine.progress.progress import (
    DatasourcePhase,
    ProgressCallback,
    ProgressEvent,
    ProgressKind,
)


@contextmanager
def rich_progress() -> Iterator[ProgressCallback]:
    """Context manager that provides a ProgressCallback rendering a live UI via Rich.

    Usage:
        with rich_progress() as progress_cb:
            manager.build_context(..., progress=progress_cb)
    """  # noqa: DOC402
    console = Console(stderr=True)
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(),
        transient=False,  # keep the progress display after finishing (change to True if you prefer)
        console=console,
    )

    # Task IDs (created lazily when we receive events)
    tasks: dict[str, TaskID] = {}

    # Some state so we can update descriptions nicely
    state = {
        "current_datasource_id": None,
        "current_phase": None,
        "datasource_index": None,
        "datasource_total": None,
        "embed_total": None,
        "persist_total": None,
    }

    def _ensure_overall_task(total: int | None) -> TaskID:
        if "overall" not in tasks:
            tasks["overall"] = progress.add_task("Datasources", total=total)
        else:
            # allow setting/updating totals later
            if total is not None:
                progress.update(tasks["overall"], total=total)
        return tasks["overall"]

    def _ensure_embed_task(total: int | None) -> TaskID:
        ds = state["current_datasource_id"] or "datasource"
        phase = state["current_phase"] or DatasourcePhase.EMBED
        desc = f"{ds} • {phase.value}"
        if "embed" not in tasks:
            tasks["embed"] = progress.add_task(desc, total=total)
        else:
            progress.update(tasks["embed"], description=desc)
            if total is not None:
                progress.update(tasks["embed"], total=total)
        return tasks["embed"]

    def _ensure_persist_task(total: int | None) -> TaskID:
        ds = state["current_datasource_id"] or "datasource"
        phase = state["current_phase"] or DatasourcePhase.PERSIST
        desc = f"{ds} • {phase.value}"
        if "persist" not in tasks:
            tasks["persist"] = progress.add_task(desc, total=total)
        else:
            progress.update(tasks["persist"], description=desc)
            if total is not None:
                progress.update(tasks["persist"], total=total)
        return tasks["persist"]

    def _update_overall_description() -> None:
        if "overall" not in tasks:
            return
        idx = state["datasource_index"]
        tot = state["datasource_total"]
        ds = state["current_datasource_id"]
        ph = state["current_phase"].value if state["current_phase"] else None

        if idx is not None and tot is not None and ds:
            suffix = f" • {ds}"
            if ph:
                suffix += f" • {ph}"
            progress.update(tasks["overall"], description=f"Datasources {idx}/{tot}{suffix}")

    def on_event(ev: ProgressEvent) -> None:
        # ---- Build-level ----
        if ev.kind == ProgressKind.BUILD_STARTED:
            _ensure_overall_task(ev.datasource_total)
            return

        if ev.kind == ProgressKind.BUILD_FINISHED:
            # Optional: mark overall as completed if totals are known.
            # (We’re already advancing it on datasource finished.)
            if ev.message:
                progress.console.print(f"{ev.message}")
            return

        # ---- Datasource lifecycle ----
        if ev.kind == ProgressKind.DATASOURCE_STARTED:
            state["current_datasource_id"] = ev.datasource_id
            state["datasource_index"] = ev.datasource_index
            state["datasource_total"] = ev.datasource_total
            state["embed_total"] = None
            state["persist_total"] = None
            _ensure_overall_task(ev.datasource_total)
            _update_overall_description()

            # Reset per-datasource tasks so each datasource starts fresh without flicker.
            # Keep the tasks and just reset progress/description.
            ds = state["current_datasource_id"] or "datasource"

            if "embed" in tasks:
                progress.update(
                    tasks["embed"], completed=0, total=None, description=f"{ds} • {DatasourcePhase.EMBED.value}"
                )

            if "persist" in tasks:
                progress.update(
                    tasks["persist"], completed=0, total=None, description=f"{ds} • {DatasourcePhase.PERSIST.value}"
                )
            return

        if ev.kind == ProgressKind.DATASOURCE_PHASE:
            # Update current phase and refresh the overall description line
            state["current_phase"] = ev.phase
            _update_overall_description()
            return

        if ev.kind == ProgressKind.DATASOURCE_FINISHED:
            idx = ev.datasource_index or 0
            tot = ev.datasource_total or 0
            ds = ev.datasource_id or "(unknown datasource)"
            status = ev.status.value if ev.status else "done"
            progress.console.print(f"{status.upper()} {idx}/{tot}: {ds}")


            # Advance overall by 1 (ok/failed/skipped are all "completed" from a progress perspective)
            _ensure_overall_task(ev.datasource_total)
            progress.advance(tasks["overall"], 1)

            # Clear phase display for next datasource
            state["current_phase"] = None
            _update_overall_description()
            return

        # ---- Chunk discovery is informative; embed total will arrive in EMBEDDING_STARTED anyway ----
        if ev.kind == ProgressKind.CHUNKS_DISCOVERED:
            # You could surface this as a message or update descriptions; optional.
            return

        # ---- Embedding ----
        if ev.kind == ProgressKind.EMBEDDING_STARTED:
            state["current_phase"] = DatasourcePhase.EMBED
            _update_overall_description()
            _ensure_embed_task(ev.total)
            state["embed_total"] = ev.total
            return

        if ev.kind == ProgressKind.EMBEDDING_PROGRESS:
            task_id = _ensure_embed_task(ev.total)
            # Use completed to avoid drift if events are throttled
            if ev.done is not None:
                progress.update(task_id, completed=ev.done)
            if ev.total is not None:
                progress.update(task_id, total=ev.total)
                state["embed_total"] = ev.total
            return

        if ev.kind == ProgressKind.EMBEDDING_FINISHED:
            # If we have an embed task, mark it complete (if total known)
            if "embed" in tasks and state["embed_total"] is not None:
                progress.update(tasks["embed"], completed=state["embed_total"])
            return

        # ---- Persistence ----
        if ev.kind == ProgressKind.PERSIST_STARTED:
            state["current_phase"] = DatasourcePhase.PERSIST
            _update_overall_description()
            _ensure_persist_task(ev.total)
            state["persist_total"] = ev.total
            return

        if ev.kind == ProgressKind.PERSIST_PROGRESS:
            task_id = _ensure_persist_task(ev.total)
            if ev.done is not None:
                progress.update(task_id, completed=ev.done)
            if ev.total is not None:
                progress.update(task_id, total=ev.total)
                state["persist_total"] = ev.total
            return

        if ev.kind == ProgressKind.PERSIST_FINISHED:
            if "persist" in tasks and state["persist_total"] is not None:
                progress.update(tasks["persist"], completed=state["persist_total"])
            return

        # Ignore unknown events by default

    with progress:
        yield on_event
