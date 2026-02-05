import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

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


def read_datasource_type_from_context_file(context_path: Path) -> DatasourceType:
    with context_path.open("r") as context_file:
        return _read_datasource_type_from_lines(context_file, source_label=str(context_path))


def read_datasource_type_from_context(context: DatasourceContext) -> DatasourceType:
    return _read_datasource_type_from_lines(
        context.context.splitlines(True),
        source_label=str(context.datasource_id),
    )


def _read_datasource_type_from_lines(lines: Iterable[str], *, source_label: str) -> DatasourceType:
    type_key = "datasource_type"
    for line in lines:
        if line.startswith(f"{type_key}: "):
            datasource_type = yaml.safe_load(line)[type_key]
            return DatasourceType(full_type=datasource_type)
    raise ValueError(f"Could not find type in context {source_label}")


def get_introspected_datasource_list(project_layout: ProjectLayout) -> list[Datasource]:
    result = []
    for dirpath, dirnames, filenames in os.walk(project_layout.output_dir):
        for context_file_name in filenames:
            if Path(context_file_name).suffix not in DatasourceId.ALLOWED_YAML_SUFFIXES:
                continue
            context_file = Path(dirpath).joinpath(context_file_name)
            relative_context_file = context_file.relative_to(project_layout.output_dir)
            try:
                result.append(
                    Datasource(
                        id=DatasourceId.from_datasource_context_file_path(relative_context_file),
                        type=read_datasource_type_from_context_file(context_file),
                    )
                )
            except ValueError as e:
                logger.debug(str(e), exc_info=True, stack_info=True)
                logger.warning(
                    f"Ignoring introspected datasource: Failed to read datasource_type from context file at {context_file.resolve()}"
                )

    return sorted(result, key=lambda ds: str(ds.id).lower())


def get_datasource_context(project_layout: ProjectLayout, datasource_id: DatasourceId) -> DatasourceContext:
    context_path = project_layout.output_dir.joinpath(datasource_id.relative_path_to_context_file())
    if not context_path.is_file():
        raise ValueError(f"Context file not found for datasource {str(datasource_id)}")

    context = context_path.read_text()
    return DatasourceContext(datasource_id=datasource_id, context=context)


def get_all_contexts(project_layout: ProjectLayout) -> list[DatasourceContext]:
    result = []
    for dirpath, dirnames, filenames in os.walk(project_layout.output_dir):
        for context_file_name in filenames:
            if (
                Path(context_file_name).suffix not in DatasourceId.ALLOWED_YAML_SUFFIXES
                or context_file_name == "all_results.yaml"
            ):
                continue
            context_file = Path(dirpath).joinpath(context_file_name)
            relative_context_file = context_file.relative_to(project_layout.output_dir)
            result.append(
                DatasourceContext(
                    datasource_id=DatasourceId.from_datasource_context_file_path(relative_context_file),
                    context=context_file.read_text(),
                )
            )
    return sorted(result, key=lambda context: str(context.datasource_id).lower())


def get_context_header_for_datasource(datasource_id: DatasourceId) -> str:
    return f"# ===== {str(datasource_id)} ====={os.linesep}"
