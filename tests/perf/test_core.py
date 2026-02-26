from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

import databao_context_engine.perf.core as perf_core
from databao_context_engine.perf.core import (
    add_attributes,
    perf_run,
    perf_span,
    set_attribute,
)
from databao_context_engine.project.layout import get_performance_logs_file


@dataclass
class DummyProjectLayout:
    project_dir: Path


def perf_file(project_dir: Path) -> Path:
    return get_performance_logs_file(project_dir)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def only(records: list[dict[str, Any]], type_: str) -> dict[str, Any]:
    matches = [r for r in records if r.get("type") == type_]
    assert len(matches) == 1, f"expected exactly 1 {type_} record, got {len(matches)}"
    return matches[0]


@dataclass(frozen=True)
class PerfRun:
    start: dict[str, Any]
    end: dict[str, Any]
    spans: list[dict[str, Any]]

    @property
    def run_id(self) -> str:
        return self.start["run_id"]


def read_run(project_dir: Path) -> PerfRun:
    path = perf_file(project_dir)
    assert path.exists(), f"perf log file not found: {path}"

    records = read_jsonl(path)
    start = only(records, "run_start")
    end = only(records, "run_end")

    assert start["run_id"] == end["run_id"], "run_start and run_end must have the same run_id"
    rid = start["run_id"]

    spans = [r for r in records if r.get("type") == "span" and r.get("run_id") == rid]
    return PerfRun(start=start, end=end, spans=spans)


def span_named(run: PerfRun, name: str) -> dict[str, Any]:
    matches = [s for s in run.spans if s.get("name") == name]
    assert len(matches) == 1, f"expected exactly 1 span named {name!r}, got {len(matches)}"
    return matches[0]


def is_number(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def test_perf_run_writes_run_start_and_run_end(tmp_path: Path) -> None:
    @perf_run(operation="unit-test")
    def f(*, project_layout: DummyProjectLayout) -> int:
        return 123

    layout = DummyProjectLayout(project_dir=tmp_path)
    assert f(project_layout=layout) == 123

    run = read_run(tmp_path)
    assert run.start["operation"] == "unit-test"
    assert isinstance(run.start.get("ts"), str)
    assert run.start["run_id"]

    assert run.end["status"] == "ok"
    assert is_number(run.end["duration_ms"])
    assert run.end["duration_ms"] >= 0

    assert run.spans == []


def test_perf_span_emits_inside_run(tmp_path: Path) -> None:
    @perf_span("test.span")
    def g() -> str:
        return "ok"

    @perf_run(operation="unit-test")
    def run_it(*, project_layout: DummyProjectLayout) -> str:
        return g()

    run_it(project_layout=DummyProjectLayout(project_dir=tmp_path))

    run = read_run(tmp_path)
    s = span_named(run, "test.span")
    assert s["status"] == "ok"
    assert s["run_id"] == run.run_id
    assert is_number(s["t_start_ms"]) and s["t_start_ms"] >= 0
    assert is_number(s["duration_ms"]) and s["duration_ms"] >= 0


def test_nested_spans_have_parent_span_id(tmp_path: Path) -> None:
    @perf_span("inner")
    def inner() -> None:
        return None

    @perf_span("outer")
    def outer() -> None:
        inner()

    @perf_run(operation="unit-test")
    def run_it(*, project_layout: DummyProjectLayout) -> None:
        outer()

    run_it(project_layout=DummyProjectLayout(project_dir=tmp_path))

    run = read_run(tmp_path)
    outer_span = span_named(run, "outer")
    inner_span = span_named(run, "inner")
    assert inner_span["parent_span_id"] == outer_span["span_id"]


def test_datasource_id_inheritance_from_parent_span(tmp_path: Path) -> None:
    @perf_span("child")
    def child() -> None:
        return None

    @perf_span("parent", datasource_id="ds-1")
    def parent() -> None:
        child()

    @perf_run(operation="unit-test")
    def run_it(*, project_layout: DummyProjectLayout) -> None:
        parent()

    run_it(project_layout=DummyProjectLayout(project_dir=tmp_path))

    run = read_run(tmp_path)
    parent_span = span_named(run, "parent")
    child_span = span_named(run, "child")

    assert parent_span["datasource_id"] == "ds-1"
    assert child_span["datasource_id"] == "ds-1"


def test_set_attribute_and_add_attributes_attach_to_current_span(tmp_path: Path) -> None:
    @perf_span("span.with.attrs")
    def work() -> str:
        set_attribute("chunk_count", 7)
        add_attributes({"context_size_bytes": 123})
        return "done"

    @perf_run(operation="unit-test")
    def run_it(*, project_layout: DummyProjectLayout) -> str:
        return work()

    assert run_it(project_layout=DummyProjectLayout(project_dir=tmp_path)) == "done"

    run = read_run(tmp_path)
    s = span_named(run, "span.with.attrs")
    assert s["attrs"]["chunk_count"] == 7
    assert s["attrs"]["context_size_bytes"] == 123


def test_error_marks_span_and_run_and_reraises(tmp_path: Path) -> None:
    @perf_span("boom")
    def boom() -> None:
        raise ValueError("nope")

    @perf_run(operation="unit-test")
    def run_it(*, project_layout: DummyProjectLayout) -> None:
        boom()

    with pytest.raises(ValueError):
        run_it(project_layout=DummyProjectLayout(project_dir=tmp_path))

    run = read_run(tmp_path)
    assert run.end["status"] == "error"
    assert run.end.get("error_type") == "ValueError"

    s = span_named(run, "boom")
    assert s["status"] == "error"
    assert s.get("error_type") == "ValueError"


def test_attrs_callable_failure_is_ignored(tmp_path: Path) -> None:
    @perf_span("span.attrs.fail", attrs=lambda *_a, **_k: {"x": 1 / 0})
    def f() -> str:
        return "ok"

    @perf_run(operation="unit-test", attrs=lambda *_a, **_k: {"x": 1 / 0})
    def run_it(*, project_layout: DummyProjectLayout) -> str:
        return f()

    assert run_it(project_layout=DummyProjectLayout(project_dir=tmp_path)) == "ok"

    run = read_run(tmp_path)
    assert run.start["attrs"] == {}
    assert run.end["status"] == "ok"

    s = span_named(run, "span.attrs.fail")
    assert s.get("attrs") == {}


def test_exporter_open_failure_disables_perf(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def bad_open(self: Any) -> None:
        raise OSError("nope")

    monkeypatch.setattr(perf_core._JsonlExporter, "open", bad_open)

    @perf_run(operation="unit-test")
    def run_it(*, project_layout: DummyProjectLayout) -> str:
        return "ok"

    assert run_it(project_layout=DummyProjectLayout(project_dir=tmp_path)) == "ok"
    assert not perf_file(tmp_path).exists()


def test_schema_contract_for_success_run(tmp_path: Path) -> None:
    @perf_span("outer", attrs=lambda *_a, **_k: {"k": "v"})
    def outer() -> None:
        @perf_span("inner")
        def inner() -> None:
            return None

        inner()

    @perf_run(operation="unit-test", attrs={"a": 1})
    def run_it(*, project_layout: DummyProjectLayout) -> None:
        outer()

    run_it(project_layout=DummyProjectLayout(project_dir=tmp_path))

    run = read_run(tmp_path)

    assert run.start["type"] == "run_start"
    assert isinstance(run.start["ts"], str)
    assert run.start["operation"] == "unit-test"
    assert isinstance(run.start["attrs"], dict)
    assert run.start["run_id"] == run.run_id

    assert run.end["type"] == "run_end"
    assert isinstance(run.end["ts"], str)
    assert run.end["run_id"] == run.run_id
    assert run.end["status"] in ("ok", "error")
    assert is_number(run.end["duration_ms"]) and run.end["duration_ms"] >= 0

    span_ids = {s["span_id"] for s in run.spans}
    assert len(span_ids) == len(run.spans)

    for s in run.spans:
        assert s["type"] == "span"
        assert s["run_id"] == run.run_id
        assert isinstance(s["span_id"], str) and s["span_id"]
        assert s.get("parent_span_id") is None or s["parent_span_id"] in span_ids

        assert "ts" not in s
        assert "t_end_ms" not in s

        assert isinstance(s["name"], str) and s["name"]
        assert is_number(s["t_start_ms"]) and s["t_start_ms"] >= 0
        assert is_number(s["duration_ms"]) and s["duration_ms"] >= 0
        assert s["status"] in ("ok", "error")
        assert isinstance(s["attrs"], dict)
