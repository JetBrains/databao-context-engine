import os
from dataclasses import dataclass
from pathlib import Path

from databao_context_engine.project.layout import ProjectLayout
from databao_context_engine.project.runs import get_run_dir, resolve_run_name
from databao_context_engine.project.types import DatasourceId


@dataclass(eq=True, frozen=True)
class DatasourceContext:
    datasource_id: DatasourceId
    # TODO: Read the context as a BuildExecutionResult instead of a Yaml string?
    context: str


def get_datasource_context(
    project_layout: ProjectLayout, datasource_id: DatasourceId, run_name: str | None = None
) -> DatasourceContext:
    run_dir = _resolve_run_dir(project_layout, run_name)

    context_path = run_dir.joinpath(datasource_id.relative_path_to_context_file())
    if not context_path.is_file():
        raise ValueError(f"Context file not found for datasource {str(datasource_id)} in run {run_dir.name}")

    context = context_path.read_text()
    return DatasourceContext(datasource_id=datasource_id, context=context)


def get_all_contexts(project_layout: ProjectLayout, run_name: str | None = None) -> list[DatasourceContext]:
    run_dir = _resolve_run_dir(project_layout, run_name)

    result = []
    for main_type_dir in sorted((p for p in run_dir.iterdir() if p.is_dir()), key=lambda p: p.name.lower()):
        for context_path in sorted(
            (p for p in main_type_dir.iterdir() if p.suffix in [".yaml", ".yml"]), key=lambda p: p.name.lower()
        ):
            result.append(
                DatasourceContext(
                    # FIXME: The extension will always be yaml here even if the datasource is a file with a different extension
                    datasource_id=DatasourceId.from_datasource_config_file_path(context_path),
                    context=context_path.read_text(),
                )
            )

    return result


def get_context_header_for_datasource(datasource_id: DatasourceId) -> str:
    return f"# ===== {str(datasource_id)} ====={os.linesep}"


def _resolve_run_dir(project_layout: ProjectLayout, run_name: str | None) -> Path:
    resolved_run_name = resolve_run_name(project_layout=project_layout, run_name=run_name)

    run_dir = get_run_dir(project_dir=project_layout.project_dir, run_name=resolved_run_name)
    if not run_dir.is_dir():
        raise ValueError(f"Run {resolved_run_name} does not exist at {run_dir.resolve()}")

    return run_dir
