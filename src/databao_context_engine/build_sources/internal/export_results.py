import logging
from pathlib import Path

from databao_context_engine.build_sources.internal.plugin_execution import BuiltDatasourceContext
from databao_context_engine.datasource_config.datasource_context import get_context_header_for_datasource
from databao_context_engine.project.layout import ALL_RESULTS_FILE_NAME
from databao_context_engine.project.types import DatasourceId
from databao_context_engine.serialization.yaml import write_yaml_to_stream

logger = logging.getLogger(__name__)


def export_build_result(output_dir: Path, result: BuiltDatasourceContext) -> Path:
    datasource_id = DatasourceId.from_string_repr(result.datasource_id)
    export_file_path = output_dir.joinpath(datasource_id.relative_path_to_context_file())

    # Make sure the parent folder exists
    export_file_path.parent.mkdir(parents=True, exist_ok=True)

    with export_file_path.open("w") as export_file:
        write_yaml_to_stream(data=result, file_stream=export_file)

    logger.info(f"Exported result to {export_file_path.resolve()}")

    return export_file_path


def append_result_to_all_results(output_dir: Path, result: BuiltDatasourceContext):
    path = output_dir.joinpath(ALL_RESULTS_FILE_NAME)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as export_file:
        export_file.write(get_context_header_for_datasource(DatasourceId.from_string_repr(result.datasource_id)))
        write_yaml_to_stream(data=result, file_stream=export_file)
        export_file.write("\n")


# Here temporarily because it needs to be reset between runs.
# A subsequent PR will remove the existence of the all_results file
def reset_all_results(output_dir: Path):
    path = output_dir.joinpath(ALL_RESULTS_FILE_NAME)
    path.unlink(missing_ok=True)
