import logging
import os
from dataclasses import dataclass
from pathlib import Path

import yaml

from databao_context_engine.datasources.types import Datasource, DatasourceId, DatasourceType
from databao_context_engine.project.layout import ProjectLayout

logger = logging.getLogger(__name__)


@dataclass(eq=True, frozen=True)
class DatasourceContext:
    """A generated Context for a Datasource.

    Attributes:
        datasource_id: The id of the datasource.
        context: The context generated for the datasource.
    """

    datasource_id: DatasourceId
    # TODO: Read the context as a BuildExecutionResult instead of a Yaml string?
    context: str


def _read_datasource_type_from_context_file(context_path: Path) -> DatasourceType:
    with context_path.open("r") as context_file:
        type_key = "datasource_type"
        for line in context_file:
            if line.startswith(f"{type_key}: "):
                datasource_type = yaml.safe_load(line)[type_key]
                return DatasourceType(full_type=datasource_type)

    raise ValueError(f"Could not find type in context file {context_path}")


def get_introspected_datasource_list(project_layout: ProjectLayout) -> list[Datasource]:
    result = []
    for main_type_dir in sorted(
        (p for p in project_layout.output_dir.iterdir() if p.is_dir()), key=lambda p: p.name.lower()
    ):
        for context_path in sorted(
            (p for p in main_type_dir.iterdir() if p.suffix in DatasourceId.ALLOWED_YAML_SUFFIXES),
            key=lambda p: p.name.lower(),
        ):
            try:
                result.append(
                    Datasource(
                        id=DatasourceId.from_datasource_config_file_path(context_path),
                        type=_read_datasource_type_from_context_file(context_path),
                    )
                )
            except ValueError as e:
                logger.debug(str(e), exc_info=True, stack_info=True)
                logger.warning(
                    f"Ignoring introspected datasource: Failed to read datasource_type from context file at {context_path.resolve()}"
                )

    return result


def get_datasource_context(project_layout: ProjectLayout, datasource_id: DatasourceId) -> DatasourceContext:
    context_path = project_layout.output_dir.joinpath(datasource_id.relative_path_to_context_file())
    if not context_path.is_file():
        raise ValueError(f"Context file not found for datasource {str(datasource_id)}")

    context = context_path.read_text()
    return DatasourceContext(datasource_id=datasource_id, context=context)


def get_all_contexts(project_layout: ProjectLayout) -> list[DatasourceContext]:
    result = []
    for main_type_dir in sorted(
        (p for p in project_layout.output_dir.iterdir() if p.is_dir()), key=lambda p: p.name.lower()
    ):
        for context_path in sorted(
            (p for p in main_type_dir.iterdir() if p.suffix in DatasourceId.ALLOWED_YAML_SUFFIXES),
            key=lambda p: p.name.lower(),
        ):
            result.append(
                DatasourceContext(
                    datasource_id=DatasourceId.from_datasource_context_file_path(context_path),
                    context=context_path.read_text(),
                )
            )

    return result


def get_context_header_for_datasource(datasource_id: DatasourceId) -> str:
    return f"# ===== {str(datasource_id)} ====={os.linesep}"
