import logging
from pathlib import Path

from databao_context_engine.build_sources.internal.plugin_execution import BuildExecutionResult
from databao_context_engine.datasource_config.datasource_context import get_context_header_for_datasource
from databao_context_engine.project.layout import ALL_RESULTS_FILE_NAME, get_output_dir
from databao_context_engine.project.types import DatasourceId
from databao_context_engine.serialisation.yaml import write_yaml_to_stream

logger = logging.getLogger(__name__)


def create_run_dir(project_dir: Path, run_name: str) -> Path:
    output_dir = get_output_dir(project_dir)

    run_dir = output_dir.joinpath(run_name)
    run_dir.mkdir(parents=True, exist_ok=False)

    return run_dir


def export_build_result(run_dir: Path, result: BuildExecutionResult):
    datasource_id = DatasourceId.from_string_repr(result.datasource_id)
    export_file_path = run_dir.joinpath(datasource_id.relative_path_to_context_file())

    # Make sure the parent folder exists
    export_file_path.parent.mkdir(exist_ok=True)

    with export_file_path.open("w") as export_file:
        write_yaml_to_stream(data=result, file_stream=export_file)

    logger.info(f"Exported result to {export_file_path.resolve()}")


def append_result_to_all_results(run_dir: Path, result: BuildExecutionResult):
    path = run_dir.joinpath(ALL_RESULTS_FILE_NAME)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as export_file:
        export_file.write(get_context_header_for_datasource(DatasourceId.from_string_repr(result.datasource_id)))
        write_yaml_to_stream(data=result, file_stream=export_file)
        export_file.write("\n")
