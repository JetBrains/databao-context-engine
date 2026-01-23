from pathlib import Path
from typing import Any

from databao_context_engine.datasource_config.add_config import get_datasource_id_for_config_file
from databao_context_engine.datasource_config.datasource_context import DatasourceContext
from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.project.layout import (
    ProjectLayout,
    create_datasource_config_file,
    get_output_dir,
    get_source_dir,
)
from databao_context_engine.project.types import DatasourceId
from databao_context_engine.serialisation.yaml import to_yaml_string


def with_config_file(
    project_dir: Path,
    full_type: str,
    datasource_name: str,
    config_content: dict[str, Any],
    overwrite_existing: bool = False,
) -> Path:
    return create_datasource_config_file(
        project_dir,
        get_datasource_id_for_config_file(DatasourceType(full_type=full_type), datasource_name),
        to_yaml_string(config_content),
        overwrite_existing=overwrite_existing,
    )


def with_raw_source_file(project_dir: Path, file_name: str, datasource_type: DatasourceType, file_content: str) -> Path:
    file_path = (
        get_source_dir(project_dir)
        .joinpath(datasource_type.main_type)
        .joinpath(file_name)
        .with_suffix(f".{datasource_type.subtype}")
    )

    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(file_content)

    return file_path


def with_output(project_layout: ProjectLayout, datasource_contexts: list[DatasourceContext]) -> Path:
    output_dir = get_output_dir(project_layout.project_dir)
    output_dir.mkdir(exist_ok=True)

    for context in datasource_contexts:
        _create_output_context(output_dir, context.datasource_id, context.context)

    return output_dir


def _create_output_context(output_dir: Path, datasource_id: DatasourceId, output: str):
    output_file = output_dir.joinpath(datasource_id.relative_path_to_context_file())
    output_file.parent.mkdir(exist_ok=True)
    output_file.write_text(output)
