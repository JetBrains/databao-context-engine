import uuid
from datetime import datetime
from pathlib import Path

import pytest
import yaml

from nemory.build_sources.internal.export_results import export_build_results
from nemory.pluginlib.build_plugin import BuildExecutionResult
from nemory.project.layout import ALL_RESULTS_FILE_NAME


@pytest.fixture
def build_start_time() -> datetime:
    return datetime(2025, 11, 13, 10, 50, 15, 275)


def assert_run_folder_exists(tmp_path: Path) -> Path:
    run_folder = tmp_path.joinpath("output").joinpath("run-2025-11-13T10:50:15")

    assert run_folder.is_dir()

    return run_folder


def test_export_build_results__empty_results(tmp_path: Path, build_start_time: datetime) -> None:
    export_build_results(tmp_path, build_start_time, [])

    run_folder = assert_run_folder_exists(tmp_path)
    assert len(list(run_folder.iterdir())) == 0


def test_export_build_results(tmp_path: Path, build_start_time: datetime) -> None:
    result_inputs = [
        BuildExecutionResult(
            id=str(uuid.uuid4()),
            name="Datasource 1",
            type="databases/my-db",
            description="My datasource description",
            version="1.0.0",
            executed_at=datetime.now(),
            result={
                "tables": {
                    "name": "table1",
                }
            },
        ),
        BuildExecutionResult(
            id=str(uuid.uuid4()),
            name="My file",
            type="files/txt",
            description="A txt file",
            version=None,
            executed_at=datetime.now(),
            result={
                "chunks": [
                    {
                        "content": "my text",
                    }
                ]
            },
        ),
    ]
    datasource_1_result_as_repr = repr(result_inputs[0].result)
    datasource_2_result_as_repr = repr(result_inputs[1].result)

    export_build_results(tmp_path, build_start_time, result_inputs)

    run_folder = assert_run_folder_exists(tmp_path)

    all_results_file = run_folder.joinpath(ALL_RESULTS_FILE_NAME)
    assert all_results_file.is_file()
    with open(all_results_file, "r") as f:
        all_results_str = f.read()

        # Asserts for the header and one yaml attribute for each data source
        assert "# ===== databases/my-db - Datasource 1 =====\n\n" in all_results_str
        assert "name: Datasource 1" in all_results_str
        assert "# ===== files/txt - My file =====\n\n" in all_results_str
        assert "name: My file" in all_results_str

    datasource_1_folder = run_folder.joinpath("databases")
    assert datasource_1_folder.is_dir() and len(list(datasource_1_folder.iterdir())) == 1
    datasource_1_file = next(datasource_1_folder.iterdir())
    assert datasource_1_file.name == "Datasource 1.yaml"
    with open(datasource_1_file, "r") as f:
        assert yaml.safe_load(f)["result"] == datasource_1_result_as_repr

    datasource_2_folder = run_folder.joinpath("files")
    assert datasource_2_folder.is_dir() and len(list(datasource_2_folder.iterdir())) == 1
    datasource_2_file = next(datasource_2_folder.iterdir())
    assert datasource_2_file.name == "My file.yaml"
    with open(datasource_2_file, "r") as f:
        assert yaml.safe_load(f)["result"] == datasource_2_result_as_repr
