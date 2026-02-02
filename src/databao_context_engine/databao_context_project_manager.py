from dataclasses import dataclass
from pathlib import Path
from typing import Any, overload

from databao_context_engine.build_sources import BuildContextResult, build_all_datasources
from databao_context_engine.databao_context_engine import DatabaoContextEngine
from databao_context_engine.datasources.check_config import (
    CheckDatasourceConnectionResult,
)
from databao_context_engine.datasources.check_config import (
    check_datasource_connection as check_datasource_connection_internal,
)
from databao_context_engine.datasources.datasource_discovery import get_datasource_list
from databao_context_engine.datasources.types import Datasource, DatasourceId
from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.project.layout import (
    ProjectLayout,
    ensure_datasource_config_file_doesnt_exist,
    ensure_project_dir,
)
from databao_context_engine.project.layout import (
    create_datasource_config_file as create_datasource_config_file_internal,
)
from databao_context_engine.serialization.yaml import to_yaml_string
from databao_context_engine.services.chunk_embedding_service import ChunkEmbeddingMode


@dataclass
class DatasourceConfigFile:
    """A datasource config file that was created by the DatabaoContextProjectManager.

    Attributes:
        datasource_id: The unique identifier for the datasource.
        config_file_path: The path to the datasource configuration file.
    """

    datasource_id: DatasourceId
    config_file_path: Path


class DatabaoContextProjectManager:
    """Project Manager for Databao Context Projects.

    This project manager is responsible for configuring and building a Databao Context Project.
    The project_dir should already have been initialized before a Project manager can be used.

    Attributes:
        project_dir: The root directory of the Databao Context Project.
    """

    project_dir: Path
    _project_layout: ProjectLayout

    def __init__(self, project_dir: Path) -> None:
        """Initialize the DatabaoContextProjectManager.

        Args:
            project_dir: The root directory of the Databao Context Project.
        """
        self._project_layout = ensure_project_dir(project_dir=project_dir)
        self.project_dir = project_dir

    def get_configured_datasource_list(self) -> list[Datasource]:
        """Return the list of datasources configured in the project.

        This method returns all datasources configured in the src folder of the project,
        no matter whether the datasource configuration is valid or not.

        Returns:
            The list of datasources configured in the project.
        """
        return get_datasource_list(self._project_layout)

    def build_context(
        self,
        datasource_ids: list[DatasourceId] | None = None,
        chunk_embedding_mode: ChunkEmbeddingMode = ChunkEmbeddingMode.EMBEDDABLE_TEXT_ONLY,
    ) -> list[BuildContextResult]:
        """Build the context for datasources in the project.

        Any datasource with an invalid configuration will be skipped.

        Args:
            datasource_ids: The list of datasource ids to build. If None, all datasources will be built.
            chunk_embedding_mode: The mode to use for chunk embedding.

        Returns:
            The list of all built results.
        """
        # TODO: Filter which datasources to build by datasource_ids
        return build_all_datasources(project_layout=self._project_layout, chunk_embedding_mode=chunk_embedding_mode)

    def check_datasource_connection(
        self, datasource_ids: list[DatasourceId] | None = None
    ) -> list[CheckDatasourceConnectionResult]:
        """Check the connection for datasources in the project.

        Args:
            datasource_ids: The list of datasource ids to check. If None, all datasources will be checked.

        Returns:
            The list of all connection check results, sorted by datasource id.
        """
        return sorted(
            check_datasource_connection_internal(
                project_layout=self._project_layout, datasource_ids=datasource_ids
            ).values(),
            key=lambda result: str(result.datasource_id),
        )

    def create_datasource_config(
        self,
        datasource_type: DatasourceType,
        datasource_name: str,
        config_content: dict[str, Any],
        overwrite_existing: bool = False,
    ) -> DatasourceConfigFile:
        """Create a new datasource configuration file in the project.

        Args:
            datasource_type: The type of the datasource to create.
            datasource_name: The name of the datasource to create.
            config_content: The content of the datasource configuration. This is a dictionary that will be written as-is in a yaml file.
                The actual content of the configuration is not checked and might not be valid for the requested type..
            overwrite_existing: Whether to overwrite an existing datasource configuration file if it already exists.

        Returns:
            The path to the created datasource configuration file.
        """
        # TODO: Before creating the datasource, validate the config content based on which plugin will be used
        config_file = _create_datasource_config_file(
            project_layout=self._project_layout,
            datasource_type=datasource_type,
            datasource_name=datasource_name,
            config_content=config_content,
            overwrite_existing=overwrite_existing,
        )

        relative_config_file = config_file.relative_to(self._project_layout.src_dir)
        return DatasourceConfigFile(
            datasource_id=DatasourceId.from_datasource_config_file_path(relative_config_file),
            config_file_path=config_file,
        )

    @overload
    def datasource_config_exists(self, *, datasource_name: str) -> bool: ...
    @overload
    def datasource_config_exists(self, *, datasource_id: DatasourceId) -> bool: ...

    def datasource_config_exists(
        self,
        *,
        datasource_name: str | None = None,
        datasource_id: DatasourceId | None = None,
    ) -> bool:
        """Check if a datasource configuration file already exists in the project.

        Args:
            datasource_name: The name of the datasource.
            datasource_id: The id of the datasource. If provided, datasource_type and datasource_name will be ignored.

        Returns:
            True if there is already a datasource configuration file for this datasource, False otherwise.

        Raises:
            ValueError: If the wrong set of arguments is provided.
        """
        if datasource_name is not None:
            relative_config_file = Path(f"{datasource_name}.yaml")
            config_file = self._project_layout.get_source_dir() / relative_config_file
            return config_file.is_file()

        if datasource_id is None:
            raise ValueError("Either datasource_id or both datasource_type and datasource_name must be provided")

        try:
            ensure_datasource_config_file_doesnt_exist(
                project_layout=self._project_layout,
                datasource_id=datasource_id,
            )
            return False
        except ValueError:
            return True

    def get_engine_for_project(self) -> DatabaoContextEngine:
        """Instantiate a DatabaoContextEngine for the project.

        Returns:
            A DatabaoContextEngine instance for the project.
        """
        return DatabaoContextEngine(project_dir=self.project_dir)


def _create_datasource_config_file(
    project_layout: ProjectLayout,
    datasource_type: DatasourceType,
    datasource_name: str,
    config_content: dict[str, Any],
    overwrite_existing: bool,
) -> Path:
    last_datasource_name = datasource_name.split("/")[-1]
    basic_config = {"type": datasource_type.full_type, "name": last_datasource_name}

    return create_datasource_config_file_internal(
        project_layout,
        f"{datasource_name}.yaml",
        to_yaml_string(basic_config | config_content),
        overwrite_existing=overwrite_existing,
    )
