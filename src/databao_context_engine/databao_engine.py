import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from databao_context_engine.datasource_config.datasource_context import (
    DatasourceContext,
    get_all_contexts,
    get_context_header_for_datasource,
    get_datasource_context,
    get_introspected_datasource_list,
)
from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.project.layout import ProjectLayout, ensure_project_dir
from databao_context_engine.project.types import Datasource, DatasourceId
from databao_context_engine.retrieve_embeddings.public.api import retrieve_embeddings


@dataclass
class ContextSearchResult:
    """The result of a search in the project's contexts.

    Attributes:
        datasource_id: The ID of the datasource that generated the result.
        datasource_type: The type of the datasource that generated the result.
        distance: The distance between the search text and the result.
        context_result: The actual content of the result that was found as a YAML string.
            This content will be a subpart of the full context of the datasource.
            In some cases, its content won't contain the exact same attributes as what can be
            found directly in the full context.
    """

    datasource_id: DatasourceId
    datasource_type: DatasourceType
    distance: float
    context_result: str


class DatabaoContextEngine:
    """Engine for reading and using the contexts generated in a Databao Context Project.

    The Databao Context Project should already have datasources configured and built (see DatabaoContextProjectManager), so that they can be used in the Engine.

    Attributes:
        project_dir: The root directory of the Databao Context Project.
    """

    project_dir: Path
    _project_layout: ProjectLayout

    def __init__(self, project_dir: Path) -> None:
        """Initialise the DatabaoContextEngine.

        Args:
            project_dir: The root directory of the Databao Context Project.
                There must be a valid DatabaoContextProject in this directory.
        """
        self._project_layout = ensure_project_dir(project_dir=project_dir)
        self.project_dir = project_dir

    def get_introspected_datasource_list(self, run_name: str | None = None) -> list[Datasource]:
        """Return the list of datasources for which a context is available.

        Returns:
            A list of the datasources for which a context is available.
        """
        return get_introspected_datasource_list(self._project_layout, run_name=run_name)

    def get_datasource_context(self, datasource_id: DatasourceId, run_name: str | None = None) -> DatasourceContext:
        """Return the context available for a given datasource.

        Args:
            datasource_id: The ID of the datasource.
            run_name: The name of the run to use to read the context. If none is provided, the latest run will be used.

        Returns:
            The context for this datasource.
        """
        return get_datasource_context(project_layout=self._project_layout, datasource_id=datasource_id)

    def get_all_contexts(self, run_name: str | None = None) -> list[DatasourceContext]:
        """Return all contexts generated in the project.

        Returns:
             A list of all contexts generated in the project.
        """
        return get_all_contexts(project_layout=self._project_layout)

    def get_all_contexts_formatted(self) -> str:
        all_contexts = self.get_all_contexts()

        all_results = os.linesep.join(
            [f"{get_context_header_for_datasource(context.datasource_id)}{context.context}" for context in all_contexts]
        )

        return all_results

    def search_context(
        self,
        retrieve_text: str,
        limit: int | None = None,
        export_to_file: bool = False,
        datasource_ids: list[DatasourceId] | None = None,
    ) -> list[ContextSearchResult]:
        """Search in the avaialable context for the closest matches to the given text.

        Args:
            retrieve_text: The text to search for in the contexts.
            run_name: The name of the run to use to read the contexts. If none is provided, the latest run will be used.
            limit: The maximum number of results to return. If None is provided, a default limit of 10 will be used.
            export_to_file: Whether the results should be exported to a file as a side-effect. If True, the results will be exported in a file in the run directory.
            datasource_ids: Not Implemented yet: providing this argument changes nothing to the search

        Returns:
            A list of the results found for the search, sorted by distance.
        """
        # TODO: Filter with datasource_ids
        # TODO: Remove the need for a run_name
        # TODO: When no run_name is required, we can extract the "export_to_file" side-effect and let the caller (the CLI) do it themselves

        results = retrieve_embeddings(
            project_layout=self._project_layout,
            retrieve_text=retrieve_text,
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
        """Not Implemented yet. This will allow to run a SQL query against a datasource (if the datasource supports it)."""
        raise NotImplementedError("Running SQL is not supported yet")
