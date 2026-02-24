from __future__ import annotations

import json
import logging
import time
import uuid
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping

from databao_context_engine.project.layout import get_performance_logs_file

logger = logging.getLogger(__name__)

_recorder_var: ContextVar["_PerfRecorder | None"] = ContextVar("dce_perf_recorder", default=None)
_current_span_var: ContextVar["_Span | None"] = ContextVar("dce_perf_span", default=None)


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve(value_or_fn: Any, *args: Any, **kwargs: Any) -> Any:
    if callable(value_or_fn):
        try:
            return value_or_fn(*args, **kwargs)
        except Exception:
            logger.debug("Perf callable failed", exc_info=True)
            return None
    return value_or_fn


def _as_dict(maybe_mapping: Any) -> dict[str, Any]:
    if not maybe_mapping:
        return {}
    try:
        return dict(maybe_mapping)
    except Exception:
        logger.debug("Perf attrs must be mapping-like", exc_info=True)
        return {}


class _JsonlExporter:
    def __init__(self, file_path: Path):
        self._file_path = file_path
        self._fp: Any | None = None

    def open(self) -> None:
        self._file_path.parent.mkdir(parents=True, exist_ok=True)
        self._fp = self._file_path.open("a", encoding="utf-8")

    def close(self) -> None:
        if self._fp is not None:
            self._fp.close()
        self._fp = None

    def write(self, record: Mapping[str, Any]) -> None:
        if self._fp is None:
            return
        try:
            line = json.dumps(record, ensure_ascii=False, separators=(",", ":"), default=str)
            self._fp.write(line + "\n")
            self._fp.flush()
        except Exception:
            logger.debug("Failed to write perf record", exc_info=True)


class _PerfRecorder:
    def __init__(self, *, exporter: _JsonlExporter, operation: str, run_attrs: Mapping[str, Any] | None = None):
        self.run_id: str = uuid.uuid4().hex
        self.operation = operation
        self._exporter = exporter
        self.run_start_perf_ns = time.perf_counter_ns()

        self.write(
            {
                "type": "run_start",
                "ts": _utc_iso(),
                "run_id": self.run_id,
                "operation": self.operation,
                "attrs": dict(run_attrs or {}),
            }
        )

    def write(self, record: Mapping[str, Any]) -> None:
        self._exporter.write(record)

    def end(self, *, status: str, error_type: str | None) -> None:
        duration_ms = (time.perf_counter_ns() - self.run_start_perf_ns) // 1_000_000
        record: dict[str, Any] = {
            "type": "run_end",
            "ts": _utc_iso(),
            "run_id": self.run_id,
            "status": status,
            "duration_ms": duration_ms,
        }
        if error_type is not None:
            record["error_type"] = error_type
        self.write(record)

    def start_span(self, *, name: str, datasource_id: str | None, attrs: Mapping[str, Any]) -> "_Span":
        parent = _current_span_var.get()
        resolved_ds = datasource_id if datasource_id is not None else (parent.datasource_id if parent else None)

        return _Span(
            recorder=self,
            name=name,
            span_id=uuid.uuid4().hex,
            parent_span_id=(parent.span_id if parent else None),
            datasource_id=resolved_ds,
            attrs=dict(attrs),
        )


@dataclass
class _Span:
    recorder: _PerfRecorder
    name: str
    span_id: str
    parent_span_id: str | None
    datasource_id: str | None
    attrs: dict[str, Any] = field(default_factory=dict)

    _start_perf_ns: int | None = None
    _span_token: Token | None = None

    def set_attribute(self, key: str, value: Any) -> None:
        self.attrs[key] = value

    def add_attributes(self, attrs: Mapping[str, Any]) -> None:
        self.attrs.update(attrs)

    def __enter__(self) -> "_Span":
        self._start_perf_ns = time.perf_counter_ns()
        self._span_token = _current_span_var.set(self)

        return self

    def __exit__(self, exc_type, exc, tb):
        end_ns = time.perf_counter_ns()
        start_ns = self._start_perf_ns or end_ns

        run_start_ns = self.recorder.run_start_perf_ns
        duration_ms = (end_ns - start_ns) // 1_000_000
        t_start_ms = round(start_ns - run_start_ns) // 1_000_000

        record: dict[str, Any] = {
            "type": "span",
            "run_id": self.recorder.run_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "datasource_id": self.datasource_id,
            "name": self.name,
            "t_start_ms": t_start_ms,
            "duration_ms": duration_ms,
            "status": "ok" if exc_type is None else "error",
            "attrs": self.attrs,
        }
        if exc_type is not None:
            record["error_type"] = exc_type.__name__

        self.recorder.write(record)

        if self._span_token is not None:
            _current_span_var.reset(self._span_token)

        return False


@contextmanager
def span(
    name: str,
    *,
    datasource_id: str | None = None,
    attrs: Mapping[str, Any] | None = None,
    **more_attrs: Any,
) -> Iterator[None]:
    recorder = _recorder_var.get()
    if recorder is None:
        yield
        return

    merged = dict(attrs or {})
    merged.update(more_attrs)

    with recorder.start_span(name=name, datasource_id=datasource_id, attrs=merged):
        yield


def set_attribute(key: str, value: Any) -> None:
    s = _current_span_var.get()
    if s is None:
        return
    s.set_attribute(key, value)


def add_attributes(attrs: Mapping[str, Any]) -> None:
    s = _current_span_var.get()
    if s is None:
        return
    s.add_attributes(attrs)


AttrsFn = Callable[..., Mapping[str, Any]]
DatasourceFn = Callable[..., str | None]
RunAttrsFn = Callable[..., Mapping[str, Any]]
AnyFn = Callable[..., Any]
Decorator = Callable[[AnyFn], AnyFn]


def perf_span(
    name: str,
    *,
    attrs: AttrsFn | None = None,
    datasource_id: str | DatasourceFn | None = None,
) -> Decorator:
    def deco(fn: AnyFn) -> AnyFn:
        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            recorder = _recorder_var.get()

            if recorder is None:
                return fn(*args, **kwargs)

            resolved_attrs = _as_dict(_resolve(attrs, *args, **kwargs))
            resolved_ds = _resolve(datasource_id, *args, **kwargs)

            if resolved_ds is None and "datasource_id" in resolved_attrs:
                resolved_ds = str(resolved_attrs.pop("datasource_id"))

            with recorder.start_span(name=name, datasource_id=resolved_ds, attrs=resolved_attrs):
                return fn(*args, **kwargs)

        return wrapper

    return deco


def perf_run(
    *,
    operation: str,
    attrs: Mapping[str, Any] | RunAttrsFn | None = None,
) -> Decorator:
    def deco(fn: AnyFn) -> AnyFn:
        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            project_layout = kwargs.get("project_layout")
            if project_layout is None:
                return fn(*args, **kwargs)
            path = get_performance_logs_file(project_layout.project_dir)

            run_attrs = _as_dict(_resolve(attrs, *args, **kwargs))

            exporter = _JsonlExporter(path)
            try:
                exporter.open()
            except Exception:
                logger.warning("Failed to open perf log at %s; perf disabled", path, exc_info=True)
                return fn(*args, **kwargs)

            recorder = _PerfRecorder(exporter=exporter, operation=operation, run_attrs=run_attrs)
            token = _recorder_var.set(recorder)

            status = "ok"
            error_type: str | None = None
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                status = "error"
                error_type = type(e).__name__
                raise
            finally:
                recorder.end(status=status, error_type=error_type)
                _recorder_var.reset(token)
                exporter.close()

        return wrapper

    return deco
