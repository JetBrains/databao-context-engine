from pathlib import Path
from typing import Any, Mapping, overload

from pydantic import TypeAdapter

from databao_context_engine.build_sources import (
    BuildDatasourceResult,
    IndexDatasourceResult,
    build_all_datasources,
    index_built_contexts,
)
from databao_context_engine.databao_context_engine import DatabaoContextEngine
from databao_context_engine.datasources.check_config import (
    CheckDatasourceConnectionResult,
    check_datasource_config_connection,
)
from databao_context_engine.datasources.check_config import (
    check_datasource_connection as check_datasource_connection_internal,
)
from databao_context_engine.datasources.config_wizard import (
    UserInputCallback,
    build_config_content_interactively,
)
from databao_context_engine.datasources.datasource_context import DatasourceContext
from databao_context_engine.datasources.datasource_discovery import get_datasource_list
from databao_context_engine.datasources.types import ConfiguredDatasource, Datasource, DatasourceId
from databao_context_engine.pluginlib.build_plugin import ConfigFile, DatasourceType
from databao_context_engine.plugins.plugin_loader import DatabaoContextPluginLoader
from databao_context_engine.project.layout import (
    ProjectLayout,
    ensure_project_dir,
)
from databao_context_engine.project.layout import (
    create_datasource_config_file as create_datasource_config_file_internal,
)
from databao_context_engine.serialization.yaml import to_yaml_string
from databao_context_engine.services.chunk_embedding_service import ChunkEmbeddingMode


class DatabaoContextProjectManager:
    """Project Manager for Databao Context Projects.

    This project manager is responsible for configuring and building a Databao Context Project.
    The project_dir should already have been initialized before a Project manager can be used.

    Attributes:
        project_dir: The root directory of the Databao Context Project.
    """

    project_dir: Path
    _project_layout: ProjectLayout

    def __init__(self, project_dir: Path, plugin_loader: DatabaoContextPluginLoader | None = None) -> None:
        """Initialize the DatabaoContextProjectManager.

        Args:
            project_dir: The root directory of the Databao Context Project.
            plugin_loader: Plugin loader which will be created anew by default unless provided.
            This object could be reused betwee project managers to reduce some overhead on the plugin discovery.
        """
        self._project_layout = ensure_project_dir(project_dir=project_dir)
        self.project_dir = project_dir
        self._plugin_loader = plugin_loader if plugin_loader else DatabaoContextPluginLoader()

    def get_configured_datasource_list(self) -> list[ConfiguredDatasource]:
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
        *,
        should_index: bool = True,
    ) -> list[BuildDatasourceResult]:
        """Build the context for datasources in the project.

        Any datasource with an invalid configuration will be skipped.

        Args:
            datasource_ids: The list of datasource ids to build. If None, all datasources will be built.
            chunk_embedding_mode: The mode to use for chunk embedding.
            should_index: Whether to build a semantic index for the context.

        Returns:
            The list of all built results.
        """
        # TODO: Filter which datasources to build by datasource_ids
        project_config = self._project_layout.read_config_file()
        return build_all_datasources(
            project_layout=self._project_layout,
            plugin_loader=self._plugin_loader,
            chunk_embedding_mode=chunk_embedding_mode,
            generate_embeddings=should_index,
            ollama_model_id=project_config.ollama_model_id,
            ollama_model_dim=project_config.ollama_model_dim,
        )

    def index_built_contexts(
        self,
        datasource_ids: list[DatasourceId] | None = None,
        chunk_embedding_mode: ChunkEmbeddingMode = ChunkEmbeddingMode.EMBEDDABLE_TEXT_ONLY,
    ) -> list[IndexDatasourceResult]:
        """Index built datasource contexts into the embeddings database.

        It reads already built context files from the output directory, chunks them using the appropriate plugin,
        embeds the chunks and persists both the chunks and embeddings.

        Args:
            datasource_ids: The list of datsource ids to index. If None, all datsources will be indexed.
            chunk_embedding_mode: The mode to use for chunk embedding.

        Returns:
            The summary of the index operation.
        """
        engine: DatabaoContextEngine = self.get_engine_for_project()
        contexts: list[DatasourceContext] = engine.get_all_contexts()

        if datasource_ids is not None:
            wanted_paths = {d.datasource_path for d in datasource_ids}
            contexts = [c for c in contexts if c.datasource_id.datasource_path in wanted_paths]

        project_config = self._project_layout.read_config_file()
        return index_built_contexts(
            project_layout=self._project_layout,
            plugin_loader=self._plugin_loader,
            contexts=contexts,
            chunk_embedding_mode=chunk_embedding_mode,
            ollama_model_id=project_config.ollama_model_id,
            ollama_model_dim=project_config.ollama_model_dim,
        )

    def check_datasource_connection(
        self, datasource_ids: list[DatasourceId] | None = None
    ) -> dict[DatasourceId, CheckDatasourceConnectionResult]:
        """Check the connection for datasources in the project.

        Args:
            datasource_ids: The list of datasource ids to check. If None, all datasources will be checked.

        Returns:
            The dict of all connection check results
        """
        return check_datasource_connection_internal(
            project_layout=self._project_layout, plugin_loader=self._plugin_loader, datasource_ids=datasource_ids
        )

    def check_datasource_config_connection(
        self,
        datasource_type: DatasourceType,
        datasource_name: str,
        config_content: ConfigFile | dict[str, Any],
    ) -> CheckDatasourceConnectionResult:
        """Validate config and check the connection for it without creating datasource.

        Args:
            datasource_type: The type of the datasource to verify.
            datasource_name: The name of the datasource to verify.
            config_content: The content of the datasource configuration to verify.

        Returns:
            The connection check result
        """
        datasource_name_without_folders = datasource_name.split("/")[-1]
        actual_config_content = self._validate_and_dump_config_content(
            config_content, datasource_name_without_folders, datasource_type, True
        )
        return check_datasource_config_connection(
            plugin_loader=self._plugin_loader,
            datasource_type=datasource_type,
            datasource_name=datasource_name,
            config_content=actual_config_content,
        )

    def create_datasource_config(
        self,
        datasource_type: DatasourceType,
        datasource_name: str,
        config_content: ConfigFile | dict[str, Any],
        overwrite_existing: bool = False,
        validate_config_content: bool = True,
    ) -> ConfiguredDatasource:
        """Create a new datasource configuration file in the project.

        The config content can be either a dict representation of the config or directly using the config type declared by a Datasource plugin.
        If the content is provided as a dict, the dict will be validated against the configuration expected by the plugin.

        Args:
            datasource_type: The type of the datasource to create.
            datasource_name: The name of the datasource to create.
            config_content: The content of the datasource configuration.
            overwrite_existing: Whether to overwrite an existing datasource configuration file if it already exists.
            validate_config_content: Whether to validate that the content of the config file is valid for that datasource type.

        Returns:
            The path to the created datasource configuration file.
        """
        datasource_name_without_folders = datasource_name.split("/")[-1]
        actual_config_content = self._validate_and_dump_config_content(
            config_content, datasource_name_without_folders, datasource_type, validate_config_content
        )

        return _create_datasource_config_file(
            project_layout=self._project_layout,
            datasource_type=datasource_type,
            datasource_name=datasource_name,
            config_content=actual_config_content,
            overwrite_existing=overwrite_existing,
        )

    def create_datasource_config_interactively(
        self,
        datasource_type: DatasourceType,
        datasource_name: str,
        user_input_callback: UserInputCallback,
        overwrite_existing: bool = False,
        validate_config_content: bool = True,
    ) -> ConfiguredDatasource:
        """Create a new datasource configuration file in the project interactively.

        Args:
            datasource_type: The type of the datasource to create.
            datasource_name: The name of the datasource to create.
            user_input_callback: A callback to ask user for an input during the creation process.
            overwrite_existing: Whether to overwrite an existing datasource configuration file if it already exists.
            validate_config_content: Whether to validate that the content of the config file is valid for that datasource type.

        Returns:
            The path to the created datasource configuration file.

        """
        config_properties = self._plugin_loader.get_config_file_structure_for_datasource_type(
            datasource_type=datasource_type
        )

        config_content = build_config_content_interactively(
            properties=config_properties, user_input_callback=user_input_callback
        )

        datasource_name_without_folders = datasource_name.split("/")[-1]
        actual_config_content = self._validate_and_dump_config_content(
            config_content, datasource_name_without_folders, datasource_type, validate_config_content
        )

        return _create_datasource_config_file(
            project_layout=self._project_layout,
            datasource_type=datasource_type,
            datasource_name=datasource_name,
            config_content=actual_config_content,
            overwrite_existing=overwrite_existing,
        )

    @overload
    def datasource_config_exists(self, *, datasource_name: str) -> DatasourceId | None: ...
    @overload
    def datasource_config_exists(self, *, datasource_id: DatasourceId) -> DatasourceId | None: ...

    def datasource_config_exists(
        self,
        *,
        datasource_name: str | None = None,
        datasource_id: DatasourceId | None = None,
    ) -> DatasourceId | None:
        """Check if a datasource configuration file already exists in the project.

        Args:
            datasource_name: The name of the datasource.
            datasource_id: The id of the datasource. If provided, datasource_type and datasource_name will be ignored.

        Returns:
            datasource_id if there is already a datasource configuration file for this datasource, None otherwise.

        Raises:
            ValueError: If the wrong set of arguments is provided.
        """
        if datasource_name is not None:
            datasource_id = DatasourceId.from_string_repr(f"{datasource_name}.yaml")
            config_file = self._project_layout.src_dir / datasource_id.relative_path_to_config_file()
            if config_file.is_file():
                return datasource_id
            return None

        if datasource_id is None:
            raise ValueError("Either datasource_id or both datasource_type and datasource_name must be provided")

        try:
            config_file = self._project_layout.src_dir.joinpath(datasource_id.relative_path_to_config_file())
            if config_file.is_file():
                raise ValueError(f"A config file already exists for {str(datasource_id)}")

            return datasource_id
        except ValueError:
            return None

    def get_config_file_path_for_datasource(self, datasource_id: DatasourceId) -> Path:
        """Return the path to the config file (or to the raw file) for the given datasource id.

        Args:
              datasource_id: The datasource id we need the config file path for.

        Returns:
              The path to the config file (or to the raw file) for the given datasource id.
        """
        return datasource_id.absolute_path_to_config_file(self._project_layout)

    def get_engine_for_project(self) -> DatabaoContextEngine:
        """Instantiate a DatabaoContextEngine for the project.

        Returns:
            A DatabaoContextEngine instance for the project.
        """
        return DatabaoContextEngine(project_dir=self.project_dir)

    def _validate_and_dump_config_content(
        self,
        config_content: ConfigFile | dict[str, Any],
        datasource_name: str,
        datasource_type: DatasourceType,
        validate_config_content: bool,
    ) -> dict[str, Any]:
        config_file_type = self._plugin_loader.get_config_file_type_for_datasource_type(datasource_type)
        config_type_adapter: TypeAdapter[ConfigFile] = TypeAdapter(config_file_type)

        if isinstance(config_content, Mapping):
            # If the config content is a Mapping, we should add in the type and name to make sure they are correct
            actual_config_content = {"type": datasource_type.full_type, "name": datasource_name}

            actual_config_content.update(config_content)
            if validate_config_content:
                config_type_adapter.validate_python(actual_config_content)
        else:
            if validate_config_content:
                config_type_adapter.validate_python(config_content)
            actual_config_content = config_type_adapter.dump_python(config_content)

        return actual_config_content


def _create_datasource_config_file(
    project_layout: ProjectLayout,
    datasource_type: DatasourceType,
    datasource_name: str,
    config_content: dict[str, Any],
    overwrite_existing: bool,
) -> ConfiguredDatasource:
    config_file = create_datasource_config_file_internal(
        project_layout,
        f"{datasource_name}.yaml",
        to_yaml_string(config_content),
        overwrite_existing=overwrite_existing,
    )

    return ConfiguredDatasource(
        datasource=Datasource(
            id=DatasourceId.from_datasource_config_file_path(project_layout, config_file),
            type=datasource_type,
        ),
        config=config_content,
    )
