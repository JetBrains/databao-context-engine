from datetime import datetime
from pathlib import Path

import yaml

from nemory.build_sources.internal.export_results import (
    append_result_to_all_results,
    create_run_dir,
    export_build_result,
)
from nemory.build_sources.internal.plugin_execution import BuildExecutionResult


def _run_dir(tmp_path: Path) -> Path:
    return tmp_path.joinpath("output").joinpath("run-2025-11-13T10:50:15")


def _make_result(*, id: str, full_type: str, payload: object) -> BuildExecutionResult:
    return BuildExecutionResult(
        datasource_id=id,
        datasource_type=full_type,
        context_built_at=datetime.now(),
        context=payload,
    )


def assert_run_folder_exists(tmp_path: Path) -> Path:
    run_folder = tmp_path.joinpath("output").joinpath("run-2025-11-13T10:50:15")

    assert run_folder.is_dir()

    return run_folder


def test_create_run_dir_creates_folder(tmp_path: Path) -> None:
    run_dir = create_run_dir(project_dir=tmp_path, run_name="run-2025-11-13T10:50:15")
    assert run_dir == _run_dir(tmp_path)
    assert run_dir.is_dir()
    assert list(run_dir.iterdir()) == []


def test_export_build_result_writes_yaml(tmp_path: Path) -> None:
    run_dir = _run_dir(tmp_path)
    run_dir.mkdir(parents=True, exist_ok=True)

    res = _make_result(
        id="databases/Datasource 1.yaml",
        full_type="databases/my-db",
        payload={"tables": [{"name": "t1"}]},
    )

    export_build_result(run_dir, res)

    out = run_dir / "databases" / "Datasource 1.yaml"
    assert out.is_file()
    data = yaml.safe_load(out.read_text())
    assert data["datasource_id"] == "databases/Datasource 1.yaml"
    assert data["datasource_type"] == "databases/my-db"
    assert data["context"] == res.context


def test_append_result_to_all_results_appends(tmp_path: Path) -> None:
    run_dir = _run_dir(tmp_path)
    run_dir.mkdir(parents=True, exist_ok=True)

    a = _make_result(id="files/A.txt", full_type="files/txt", payload={"chunks": 1})
    b = _make_result(id="databases/B.yaml", full_type="databases/postgres", payload={"ok": True})

    append_result_to_all_results(run_dir, a)
    append_result_to_all_results(run_dir, b)

    all_file = run_dir / "all_results.yaml"
    assert all_file.is_file()
    txt = all_file.read_text()

    assert "# ===== files/A.txt =====\n" in txt
    assert "datasource_id: files/A.txt" in txt
    assert "datasource_type: files/txt" in txt
    assert "# ===== databases/B.yaml =====\n" in txt
    assert "datasource_id: databases/B.yaml" in txt
    assert "datasource_type: databases/postgres" in txt
