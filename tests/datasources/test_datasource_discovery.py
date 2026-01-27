from pathlib import Path

import pytest

from databao_context_engine.datasources.datasource_discovery import (
    _load_datasource_descriptor,
    discover_datasources,
)
from databao_context_engine.datasources.types import DatasourceKind


def _mk(p: Path, text: str = "x") -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")
    return p


def _snapshot(rows):
    return [(r.main_type, r.path.name, r.kind) for r in rows]


def test_missing_src_raises(tmp_path: Path):
    with pytest.raises(ValueError):
        discover_datasources(tmp_path)


def test_empty_src_returns_empty(tmp_path: Path):
    (tmp_path / "src").mkdir()
    assert discover_datasources(tmp_path) == []


def test_orders_by_dir_then_filename(tmp_path: Path):
    _mk(tmp_path / "src" / "A" / "1.txt")
    _mk(tmp_path / "src" / "b" / "0.txt")

    rows = discover_datasources(tmp_path)
    assert _snapshot(rows) == [
        ("A", "1.txt", DatasourceKind.FILE),
        ("b", "0.txt", DatasourceKind.FILE),
    ]


def test_files_dir_treats_everything_as_FILE_including_yaml(tmp_path: Path):
    _mk(tmp_path / "src" / "files" / "x.yaml", "k: v")
    _mk(tmp_path / "src" / "files" / "y.txt", "hello")

    rows = discover_datasources(tmp_path)
    kinds = {r.path.name: r.kind for r in rows}
    mains = {r.path.name: r.main_type for r in rows}

    assert kinds["x.yaml"] == DatasourceKind.FILE
    assert kinds["y.txt"] == DatasourceKind.FILE
    assert mains["x.yaml"] == "files"
    assert mains["y.txt"] == "files"


def test_yaml_elsewhere_is_CONFIG(tmp_path: Path):
    _mk(tmp_path / "src" / "databases" / "pg.yml", "type: postgres")

    rows = discover_datasources(tmp_path)
    assert _snapshot(rows) == [
        ("databases", "pg.yml", DatasourceKind.CONFIG),
    ]


def test_non_yaml_elsewhere_with_extension_is_FILE(tmp_path: Path):
    _mk(tmp_path / "src" / "databases" / "readme.md", "# hi")

    rows = discover_datasources(tmp_path)
    assert _snapshot(rows) == [
        ("databases", "readme.md", DatasourceKind.FILE),
    ]


def test_skips_files_without_extension(tmp_path: Path):
    _mk(tmp_path / "src" / "misc" / "NOEXT", "data")

    rows = discover_datasources(tmp_path)
    assert rows == []


def test_load_descriptor_files_dir_yaml_is_FILE(tmp_path: Path):
    p = _mk(tmp_path / "src" / "files" / "conf.yaml", "k: v")
    d = _load_datasource_descriptor(p)
    assert d is not None
    assert d.kind == DatasourceKind.FILE
    assert d.main_type == "files"
    assert d.path.name == "conf.yaml"


def test_load_descriptor_yaml_elsewhere_is_CONFIG(tmp_path: Path):
    p = _mk(tmp_path / "src" / "databases" / "ds.yaml", "type: pg")
    d = _load_datasource_descriptor(p)
    assert d is not None
    assert d.kind == DatasourceKind.CONFIG
    assert d.main_type == "databases"
    assert d.path.name == "ds.yaml"


def test_load_descriptor_no_extension_returns_none(tmp_path: Path):
    p = _mk(tmp_path / "src" / "misc" / "NOEXT", "data")
    assert _load_datasource_descriptor(p) is None
