from dataclasses import dataclass
from pathlib import Path
from typing import Any, overload

from databao_context_engine.build_sources.public.api import BuildContextResult, build_all_datasources
from databao_context_engine.databao_engine import DatabaoContextEngine
from databao_context_engine.datasource_config.add_config import (
    create_datasource_config_file,
    get_datasource_id_for_config_file,
)
from databao_context_engine.datasource_config.check_config import (
    CheckDatasourceConnectionResult,
)
from databao_context_engine.datasource_config.check_config import (
    check_datasource_connection as check_datasource_connection_internal,
)
from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.project.datasource_discovery import get_datasource_list
from databao_context_engine.project.layout import ensure_datasource_config_file_doesnt_exist, ensure_project_dir
from databao_context_engine.project.types import Datasource, DatasourceId
from databao_context_engine.services.chunk_embedding_service import ChunkEmbeddingMode


@dataclass
class DatasourceConfigFile:
    datasource_id: DatasourceId
    config_file_path: Path


class DatabaoContextProjectManager:
    project_dir: Path

    def __init__(self, project_dir: Path) -> None:
        ensure_project_dir(project_dir=project_dir)
        self.project_dir = project_dir

    def get_configured_datasource_list(self) -> list[Datasource]:
        return get_datasource_list(self.project_dir)

    def build_context(
        self,
        datasource_ids: list[DatasourceId] | None = None,
        chunk_embedding_mode: ChunkEmbeddingMode = ChunkEmbeddingMode.EMBEDDABLE_TEXT_ONLY,
    ) -> list[BuildContextResult]:
        # TODO: Filter which datasources to build by datasource_ids
        return build_all_datasources(project_dir=self.project_dir, chunk_embedding_mode=chunk_embedding_mode)

    def check_datasource_connection(
        self, datasource_ids: list[DatasourceId] | None = None
    ) -> list[CheckDatasourceConnectionResult]:
        return sorted(
            check_datasource_connection_internal(project_dir=self.project_dir, datasource_ids=datasource_ids).values(),
            key=lambda result: str(result.datasource_id),
        )

    def create_datasource_config(
        self,
        datasource_type: DatasourceType,
        datasource_name: str,
        config_content: dict[str, Any],
        overwrite_existing: bool = False,
    ) -> DatasourceConfigFile:
        # TODO: Before creating the datasource, validate the config content based on which plugin will be used
        config_file_path = create_datasource_config_file(
            project_dir=self.project_dir,
            datasource_type=datasource_type,
            datasource_name=datasource_name,
            config_content=config_content,
            overwrite_existing=overwrite_existing,
        )

        return DatasourceConfigFile(
            datasource_id=DatasourceId.from_datasource_config_file_path(config_file_path),
            config_file_path=config_file_path,
        )

    @overload
    def datasource_config_exists(self, *, datasource_type: DatasourceType, datasource_name: str) -> bool: ...
    @overload
    def datasource_config_exists(self, *, datasource_id: DatasourceId) -> bool: ...

    def datasource_config_exists(
        self,
        *,
        datasource_type: DatasourceType | None = None,
        datasource_name: str | None = None,
        datasource_id: DatasourceId | None = None,
    ) -> bool:
        if datasource_type is not None and datasource_name is not None:
            datasource_id = get_datasource_id_for_config_file(datasource_type, datasource_name)
        elif datasource_id is None:
            raise ValueError("Either datasource_id or both datasource_type and datasource_name must be provided")

        try:
            ensure_datasource_config_file_doesnt_exist(
                project_dir=self.project_dir,
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
