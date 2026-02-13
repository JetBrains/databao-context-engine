import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from databao_context_engine.datasources.datasource_context import (
    DatasourceContext,
    get_all_contexts,
    get_context_header_for_datasource,
    get_datasource_context,
    get_introspected_datasource_list,
)
from databao_context_engine.datasources.execute_sql_query import run_sql
from databao_context_engine.datasources.types import Datasource, DatasourceId
from databao_context_engine.plugin_loader import DatabaoContextPluginLoader
from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.pluginlib.sql.sql_types import SqlExecutionResult
from databao_context_engine.project.layout import ProjectLayout, ensure_project_dir
from databao_context_engine.retrieve_embeddings import retrieve_embeddings


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
    _plugin_loader: DatabaoContextPluginLoader

    def __init__(self, project_dir: Path, plugin_loader: DatabaoContextPluginLoader | None = None) -> None:
        """Initialize the DatabaoContextEngine.

        Args:
            project_dir: The root directory of the Databao Context Project.
                There must be a valid DatabaoContextProject in this directory.
            plugin_loader: Optional plugin loader to use for loading plugins.
        """
        self._project_layout = ensure_project_dir(project_dir=project_dir)
        self.project_dir = project_dir
        self._plugin_loader = plugin_loader or DatabaoContextPluginLoader()

    def get_introspected_datasource_list(self) -> list[Datasource]:
        """Return the list of datasources for which a context is available.

        Returns:
            A list of the datasources for which a context is available.
        """
        return get_introspected_datasource_list(self._project_layout)

    def get_datasource_context(self, datasource_id: DatasourceId) -> DatasourceContext:
        """Return the context available for a given datasource.

        Args:
            datasource_id: The ID of the datasource.

        Returns:
            The context for this datasource.
        """
        return get_datasource_context(project_layout=self._project_layout, datasource_id=datasource_id)

    def get_all_contexts(self) -> list[DatasourceContext]:
        """Return all contexts generated in the project.

        Returns:
             A list of all contexts generated in the project.
        """
        return get_all_contexts(project_layout=self._project_layout)

    def get_all_contexts_formatted(self) -> str:
        """Return a fprmatted string of all datasource contexts in the project.

        The returned string is a concatenation of all datasource contexts, adding a header to separate each context.

        Returns:
            A fprmatted string of all datasource contexts in the project.
        """
        all_contexts = self.get_all_contexts()

        return os.linesep.join(
            [f"{get_context_header_for_datasource(context.datasource_id)}{context.context}" for context in all_contexts]
        )

    def search_context(
        self,
        retrieve_text: str,
        limit: int | None = None,
        datasource_ids: list[DatasourceId] | None = None,
    ) -> list[ContextSearchResult]:
        """Search in the available context for the closest matches to the given text.

        Args:
            retrieve_text: The text to search for in the contexts.
            limit: The maximum number of results to return. If None is provided, a default limit of 10 will be used.
            datasource_ids: If provided, the search results will only come from the datasources with these IDs.

        Returns:
            A list of the results found for the search, sorted by distance.
        """
        project_config = self._project_layout.read_config_file()
        results = retrieve_embeddings(
            project_layout=self._project_layout,
            retrieve_text=retrieve_text,
            limit=limit,
            datasource_ids=datasource_ids,
            ollama_model_id=project_config.ollama_model_id,
            ollama_model_dim=project_config.ollama_model_dim,
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

    def run_sql(
        self,
        datasource_id: DatasourceId,
        sql: str,
        params: list[Any] | None = None,
        read_only: bool = True,
    ) -> SqlExecutionResult:
        """Execute a SQL query against a datasource if it supports it.

        - Optional per plugin: raises NotSupportedError for datasources that don’t support SQL.
        - Read-only by default: set read_only=False to permit mutating statements.

        Returns:
            Sql execution result containing columns and rows.
        """
        return run_sql(self._project_layout, self._plugin_loader, datasource_id, sql, params, read_only)
