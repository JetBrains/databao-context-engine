import logging
from pathlib import Path

from databao_context_engine.build_sources.plugin_execution import BuiltDatasourceContext
from databao_context_engine.datasources.types import DatasourceId
from databao_context_engine.project.layout import DEPRECATED_ALL_RESULTS_FILE_NAME, ProjectLayout
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


def delete_all_results_file(project_layout: ProjectLayout) -> None:
    """Deletes the all_results.yaml file we were previously writing.

    We're keeping this method for now, to make sure that older projects
    don't keep an outdated "all_results.yaml" file in their output folder.

    We should be able to remove this code once we don't expect projects with an "all_results.yaml" file to exist anymore.
    """
    path = project_layout.output_dir.joinpath(DEPRECATED_ALL_RESULTS_FILE_NAME)
    path.unlink(missing_ok=True)
