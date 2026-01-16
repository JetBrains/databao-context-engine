import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from databao_context_engine.datasource_config.datasource_context import (
    DatasourceContext,
    get_all_contexts,
    get_context_header_for_datasource,
    get_datasource_context,
)
from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.project.datasource_discovery import get_datasource_list
from databao_context_engine.project.types import DatasourceId, Datasource
from databao_context_engine.project.layout import ensure_project_dir
from databao_context_engine.retrieve_embeddings.public.api import retrieve_embeddings


@dataclass
class ContextSearchResult:
    datasource_id: DatasourceId
    datasource_type: DatasourceType
    distance: float
    context_result: str


class DatabaoContextEngine:
    project_dir: Path

    def __init__(self, project_dir: Path) -> None:
        ensure_project_dir(project_dir=project_dir)
        self.project_dir = project_dir

    def get_datasource_list(self) -> list[Datasource]:
        # TODO: Should this return the list of built datasources rather than the list of datasources within the src folder?
        return get_datasource_list(self.project_dir)

    def get_datasource_context(self, datasource_id: DatasourceId, run_name: str | None = None) -> DatasourceContext:
        return get_datasource_context(project_dir=self.project_dir, datasource_id=datasource_id, run_name=run_name)

    def get_all_contexts(self, run_name: str | None = None) -> list[DatasourceContext]:
        return get_all_contexts(project_dir=self.project_dir, run_name=run_name)

    def get_all_contexts_formatted(self, run_name: str | None = None) -> str:
        all_contexts = self.get_all_contexts(run_name=run_name)

        all_results = os.linesep.join(
            [f"{get_context_header_for_datasource(context.datasource_id)}{context.context}" for context in all_contexts]
        )

        return all_results

    def search_context(
        self,
        retrieve_text: str,
        run_name: str | None,
        limit: int | None,
        export_to_file: bool,
        datasource_ids: list[DatasourceId] | None = None,
    ) -> list[ContextSearchResult]:
        # TODO: Filter with datasource_ids
        # TODO: Remove the need for a run_name

        results = retrieve_embeddings(
            project_dir=self.project_dir,
            retrieve_text=retrieve_text,
            run_name=run_name,
            limit=limit,
            export_to_file=export_to_file,
        )

        return [
            ContextSearchResult(
                datasource_id=result.datasource_id,
                datasource_type=result.datasource_type,
                distance=result.cosine_distance,
                context_result=result.display_text,
            )
            for result in results
        ]

    def run_sql(self, datasource_id: DatasourceId, sql: str, params: list[str]) -> dict[str, Any]:
        raise NotImplementedError("Running SQL is not supported yet")
