from __future__ import annotations

import logging
import sys
from contextlib import contextmanager
from typing import Callable, Iterator, Optional, TypedDict

from databao_context_engine.progress.progress import (
    ProgressCallback,
    ProgressEvent,
    ProgressKind,
)

_DESCRIPTION_COL_WIDTH = 50


def _datasource_label(ds_id: str | None) -> str:
    return ds_id or "datasource"


def _noop_progress_cb(_: ProgressEvent) -> None:
    return


class _UIState(TypedDict):
    datasource_index: int | None
    datasource_total: int | None
    last_percent: int


@contextmanager
def rich_progress() -> Iterator[ProgressCallback]:
    try:
        from rich.console import Console
        from rich.progress import (
            BarColumn,
            Progress,
            ProgressColumn,
            SpinnerColumn,
            TaskID,
            TaskProgressColumn,
            TextColumn,
            TimeRemainingColumn,
        )
        from rich.table import Column
        from rich.text import Text
    except ImportError:
        yield _noop_progress_cb
        return

    interactive = sys.stderr.isatty()
    if not interactive:
        yield _noop_progress_cb
        return

    class _EtaExceptOverallColumn(ProgressColumn):
        def __init__(self, overall_task_id_getter: Callable[[], Optional[TaskID]]):
            super().__init__()
            self._overall_task_id_getter = overall_task_id_getter
            self._eta = TimeRemainingColumn()

        def render(self, task) -> Text:
            overall_id = self._overall_task_id_getter()
            if overall_id is not None and task.id == overall_id:
                return Text("")
            return self._eta.render(task)

    console = Console(stderr=True)

    tasks: dict[str, TaskID] = {}
    ui_state: _UIState = {
        "datasource_index": None,
        "datasource_total": None,
        "last_percent": 0,
    }

    progress = Progress(
        SpinnerColumn(),
        TextColumn(
            "[progress.description]{task.description}",
            table_column=Column(width=_DESCRIPTION_COL_WIDTH, overflow="ellipsis", no_wrap=True),
        ),
        BarColumn(),
        TaskProgressColumn(),
        _EtaExceptOverallColumn(lambda: tasks.get("overall")),
        transient=True,
        console=console,
        redirect_stdout=True,
        redirect_stderr=True,
    )

    def _get_or_create_overall_task(total: int | None) -> TaskID:
        if "overall" not in tasks:
            tasks["overall"] = progress.add_task("Datasources", total=total)
        else:
            if total is not None:
                progress.update(tasks["overall"], total=total)
        return tasks["overall"]

    def _get_or_create_datasource_task() -> TaskID:
        if "datasource" not in tasks:
            tasks["datasource"] = progress.add_task("Datasource", total=100.0)
        return tasks["datasource"]

    def _set_datasource_percent(percent: float) -> None:
        task_id = _get_or_create_datasource_task()
        clamped = max(0.0, min(100.0, percent))
        progress.update(task_id, completed=clamped)

    def _update_overall_description() -> None:
        if "overall" not in tasks:
            return
        idx = ui_state["datasource_index"]
        tot = ui_state["datasource_total"]

        if idx is not None and tot is not None:
            progress.update(tasks["overall"], description=f"Datasources {idx}/{tot}")

    def on_event(ev: ProgressEvent) -> None:
        match ev.kind:
            case ProgressKind.TASK_STARTED:
                _get_or_create_overall_task(ev.datasource_total)
                return
            case ProgressKind.TASK_FINISHED:
                if ev.message:
                    progress.console.print(f"{ev.message}")
                return
            case ProgressKind.DATASOURCE_STARTED:
                ui_state["datasource_index"] = ev.datasource_index
                ui_state["datasource_total"] = ev.datasource_total
                ui_state["last_percent"] = 0
                _get_or_create_overall_task(ev.datasource_total)
                _update_overall_description()

                ds = _datasource_label(ev.datasource_id)

                task_id = _get_or_create_datasource_task()
                progress.reset(task_id, completed=0, total=100.0, description=f"{ds}")
                return
            case ProgressKind.DATASOURCE_FINISHED:
                idx = ev.datasource_index or 0
                tot = ev.datasource_total or 0
                ds = ev.datasource_id or "(unknown datasource)"
                status = ev.status.value if ev.status else "done"
                progress.console.print(f"{status.upper()} {idx}/{tot}: {ds}")

                _set_datasource_percent(100.0)

                _get_or_create_overall_task(ev.datasource_total)
                progress.advance(tasks["overall"], 1)

                _update_overall_description()
                return
            case ProgressKind.DATASOURCE_PROGRESS:
                if ev.percent is not None:
                    pct = int(ev.percent)
                    pct = max(ui_state["last_percent"], pct)
                    ui_state["last_percent"] = pct
                    _set_datasource_percent(float(pct))
                return

    root = logging.getLogger()
    prev_level = root.level
    prev_handlers = list(root.handlers)

    prev_disable_level = logging.root.manager.disable
    logging.disable(logging.CRITICAL)

    try:
        with progress:
            yield on_event
    finally:
        logging.disable(prev_disable_level)

        for h in list(root.handlers):
            root.removeHandler(h)
        for h in prev_handlers:
            root.addHandler(h)
        root.setLevel(prev_level)
