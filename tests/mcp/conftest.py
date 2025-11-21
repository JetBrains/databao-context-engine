from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from nemory.project.layout import ALL_RESULTS_FILE_NAME, get_output_dir
from nemory.storage.repositories.run_repository import RunRepository


@dataclass(kw_only=True, frozen=True)
class Run:
    run_dir: Path
    run_build_time: datetime
    all_results_file_content: str


@dataclass(kw_only=True)
class ProjectWithRuns:
    project_dir: Path
    runs: list[Run]


def _create_run_dir(output_path: Path, build_time: datetime) -> Run:
    # TODO: Add the run in the DB and use the run name from there
    run_dir = output_path.joinpath(RunRepository.generate_run_dir_name(build_time))
    run_dir.mkdir()

    all_results_content = _create_all_results_file(run_dir, build_time)

    return Run(run_dir=run_dir, run_build_time=build_time, all_results_file_content=all_results_content)


def _create_all_results_file(run_dir: Path, build_time: datetime) -> str:
    all_results_file_path = run_dir.joinpath(ALL_RESULTS_FILE_NAME)
    file_content = (
        "# ===== Dummy - Dummy =====\n\n"
        "# This file contains all the results of the build process.\n"
        f"# Generated at: {build_time.isoformat()}"
    )

    with open(all_results_file_path, "w") as file:
        file.write(file_content)

    return file_content


@pytest.fixture
def project_with_runs(project_path: Path) -> ProjectWithRuns:
    output_dir = get_output_dir(project_path)
    output_dir.mkdir()

    run_1 = _create_run_dir(output_dir, datetime.now() - timedelta(days=10))
    run_2 = _create_run_dir(output_dir, datetime.now())
    run_3 = _create_run_dir(output_dir, datetime.now() - timedelta(days=3))

    return ProjectWithRuns(project_dir=project_path, runs=[run_1, run_2, run_3])
