import logging
from datetime import datetime
from pathlib import Path
from typing import TextIO

import yaml

from nemory.pluginlib.build_plugin import BuildExecutionResult
from nemory.project.layout import ALL_RESULTS_FILE_NAME, get_output_dir, get_run_dir_name

logger = logging.getLogger(__name__)


def export_build_results(project_dir: Path, build_start_time: datetime, results: list[BuildExecutionResult]) -> None:
    run_dir = _create_run_dir(project_dir, build_start_time)

    for result in results:
        _export_build_result(run_dir, result)

    _export_to_all_results_file(run_dir, results)


def _create_run_dir(project_dir: Path, build_start_time: datetime) -> Path:
    output_dir = get_output_dir(project_dir)

    run_dir = output_dir.joinpath(get_run_dir_name(build_start_time))
    run_dir.mkdir(parents=True, exist_ok=False)

    return run_dir


def _export_build_result(run_dir: Path, result: BuildExecutionResult):
    export_file_path = _get_result_export_file_path(run_dir, result)

    with export_file_path.open("w") as export_file:
        _write_result_in_file(export_file, result)

    logger.info(f"Exported result to {export_file_path.resolve()}")


def _export_to_all_results_file(run_dir: Path, results: list[BuildExecutionResult]):
    if len(results) == 0:
        return

    with run_dir.joinpath(ALL_RESULTS_FILE_NAME).open("w") as all_results_file:
        for result in results:
            all_results_file.write(f"# ===== {result.type} - {result.name} =====\n\n")
            _write_result_in_file(all_results_file, result)
            all_results_file.write("\n")


def _write_result_in_file(export_file: TextIO, result: BuildExecutionResult):
    yaml.safe_dump(result._to_yaml_serializable(), export_file, sort_keys=False)


def _get_result_export_file_path(run_dir: Path, result: BuildExecutionResult) -> Path:
    folder_name = result.type.split("/")[0]

    folder = run_dir.joinpath(folder_name)
    folder.mkdir(exist_ok=True)

    export_filename = _get_result_export_filename(result)

    return folder.joinpath(export_filename)


def _get_result_export_filename(result: BuildExecutionResult) -> str:
    return f"{result.name}.yaml"
