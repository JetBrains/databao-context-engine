import logging
from pathlib import Path

from nemory.pluginlib.build_plugin import BuildExecutionResult
from nemory.project.layout import get_output_dir
from nemory.serialisation.yaml import write_yaml_to_stream

logger = logging.getLogger(__name__)


def create_run_dir(project_dir: Path, run_name: str) -> Path:
    output_dir = get_output_dir(project_dir)

    run_dir = output_dir.joinpath(run_name)
    run_dir.mkdir(parents=True, exist_ok=False)

    return run_dir


def export_build_result(run_dir: Path, result: BuildExecutionResult):
    export_file_path = _get_result_export_file_path(run_dir, result)

    with export_file_path.open("w") as export_file:
        write_yaml_to_stream(data=result, file_stream=export_file)

    logger.info(f"Exported result to {export_file_path.resolve()}")


def append_result_to_all_results(run_dir: Path, result: BuildExecutionResult):
    path = run_dir.joinpath("all_results.yaml")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as export_file:
        export_file.write(f"# ===== {result.type} - {result.name} =====\n")
        write_yaml_to_stream(data=result, file_stream=export_file)
        export_file.write("\n")


def _get_result_export_file_path(run_dir: Path, result: BuildExecutionResult) -> Path:
    folder_name = result.type.split("/")[0]

    folder = run_dir.joinpath(folder_name)
    folder.mkdir(exist_ok=True)

    export_filename = _get_result_export_filename(result)

    return folder.joinpath(export_filename)


def _get_result_export_filename(result: BuildExecutionResult) -> str:
    return f"{result.name}.yaml"
