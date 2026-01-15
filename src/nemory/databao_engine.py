from dataclasses import dataclass
from pathlib import Path
from typing import Any

from nemory.pluginlib.build_plugin import DatasourceType
from nemory.project.datasource_discovery import Datasource, DatasourceId
from nemory.project.layout import ensure_project_dir
from nemory.retrieve_embeddings.public.api import retrieve_embeddings


@dataclass
class DatasourceContext:
    datasource_id: DatasourceId
    # TODO: Read the context as a BuildExecutionResult instead of a Yaml string?
    context: str


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
        raise NotImplementedError("Get the datasource list is not supported yet")

    def get_datasource_context(self, datasource_id: DatasourceId, run_name: str | None = None) -> DatasourceContext:
        raise NotImplementedError("Retrieving datasource context is not supported yet")

    def get_all_contexts(self, run_name: str | None = None) -> list[DatasourceContext]:
        raise NotImplementedError("Retrieving all contexts is not supported yet")

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
