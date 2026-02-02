from pathlib import Path

from databao_context_engine.datasources.datasource_discovery import (
    _load_datasource_descriptor,
    discover_datasources,
)
from databao_context_engine.datasources.types import DatasourceId, DatasourceKind
from databao_context_engine.project.layout import ProjectLayout


def _mk(p: Path, text: str = "x") -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")
    return p


def _snapshot(rows):
    return [(r.datasource_id, r.path.name, r.kind) for r in rows]


def test_empty_src_returns_empty(project_layout: ProjectLayout):
    assert discover_datasources(project_layout) == []


def test_orders_by_dir_then_filename(project_layout: ProjectLayout):
    _mk(project_layout.src_dir / "A" / "1.txt")
    _mk(project_layout.src_dir / "b" / "0.txt")

    rows = discover_datasources(project_layout)
    assert _snapshot(rows) == [
        (
            DatasourceId(
                datasource_path="A/1",
                config_file_suffix=".txt",
            ),
            "1.txt",
            DatasourceKind.FILE,
        ),
        (
            DatasourceId(
                datasource_path="b/0",
                config_file_suffix=".txt",
            ),
            "0.txt",
            DatasourceKind.FILE,
        ),
    ]


def test_files_dir_treats_everything_as_FILE_including_yaml(project_layout: ProjectLayout):
    _mk(project_layout.src_dir / "files" / "x.yaml", "k: v")
    _mk(project_layout.src_dir / "files" / "y.txt", "hello")

    rows = discover_datasources(project_layout)
    kinds = {r.path.name: r.kind for r in rows}

    assert kinds["x.yaml"] == DatasourceKind.FILE
    assert kinds["y.txt"] == DatasourceKind.FILE


def test_yaml_elsewhere_is_CONFIG(project_layout: ProjectLayout):
    _mk(project_layout.src_dir / "databases" / "pg.yml", "type: postgres")

    rows = discover_datasources(project_layout)
    assert _snapshot(rows) == [
        (
            DatasourceId(
                datasource_path="databases/pg",
                config_file_suffix=".yml",
            ),
            "pg.yml",
            DatasourceKind.CONFIG,
        ),
    ]


def test_non_yaml_elsewhere_with_extension_is_FILE(project_layout: ProjectLayout):
    _mk(project_layout.src_dir / "databases" / "readme.md", "# hi")

    rows = discover_datasources(project_layout)
    assert _snapshot(rows) == [
        (
            DatasourceId(
                datasource_path="databases/readme",
                config_file_suffix=".md",
            ),
            "readme.md",
            DatasourceKind.FILE,
        ),
    ]


def test_skips_files_without_extension(project_layout: ProjectLayout):
    _mk(project_layout.src_dir / "misc" / "NOEXT", "data")

    rows = discover_datasources(project_layout)
    assert rows == []


def test_load_descriptor_files_dir_yaml_is_FILE(project_layout: ProjectLayout):
    p = _mk(project_layout.src_dir / "files" / "conf.yaml", "k: v")
    d = _load_datasource_descriptor(project_layout, p)
    assert d is not None
    assert d.kind == DatasourceKind.FILE
    assert d.path.name == "conf.yaml"


def test_load_descriptor_yaml_elsewhere_is_CONFIG(project_layout: ProjectLayout):
    p = _mk(project_layout.src_dir / "databases" / "ds.yaml", "type: pg")
    d = _load_datasource_descriptor(project_layout, p)
    assert d is not None
    assert d.kind == DatasourceKind.CONFIG
    assert d.path.name == "ds.yaml"


def test_load_descriptor_no_extension_returns_none(project_layout: ProjectLayout):
    p = _mk(project_layout.src_dir / "misc" / "NOEXT", "data")
    assert _load_datasource_descriptor(project_layout, p) is None
