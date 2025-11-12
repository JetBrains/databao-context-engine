import logging
from datetime import datetime
from pathlib import Path

import yaml

from nemory.pluginlib.build_plugin import BuildExecutionResult
from nemory.project.layout import get_output_dir

logger = logging.getLogger(__name__)


def export_build_results(project_dir: Path, build_start_time: datetime, results: list[BuildExecutionResult]) -> None:
    run_dir = _create_run_dir(project_dir, build_start_time)

    for result in results:
        _export_build_result(run_dir, result)


def _create_run_dir(project_dir: Path, build_start_time: datetime) -> Path:
    output_dir = get_output_dir(project_dir)

    run_dir = output_dir.joinpath(f"run-{build_start_time.isoformat(timespec='seconds')}")
    run_dir.mkdir(parents=True, exist_ok=False)

    return run_dir


def _export_build_result(run_dir: Path, result: BuildExecutionResult):
    export_file_path = _get_result_export_file_path(run_dir, result)

    with export_file_path.open("w") as export_file:
        yaml.safe_dump(result._to_yaml_serializable(), export_file, sort_keys=False)

    logger.info(f"Exported result to {export_file_path.resolve()}")


def _get_result_export_file_path(run_dir: Path, result: BuildExecutionResult) -> Path:
    folder_name = result.type.split("/")[0]

    folder = run_dir.joinpath(folder_name)
    folder.mkdir(exist_ok=True)

    export_filename = _get_result_export_filename(result)

    return folder.joinpath(export_filename)


def _get_result_export_filename(result: BuildExecutionResult) -> str:
    return f"{result.name}.yaml"
