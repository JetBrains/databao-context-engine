from dataclasses import dataclass
from pathlib import Path
from typing import Any

from databao_context_engine.build_sources.public.api import BuildContextResult, build_all_datasources
from databao_context_engine.datasource_config.add_config import create_datasource_config_file
from databao_context_engine.datasource_config.check_config import (
    CheckDatasourceConnectionResult,
    check_datasource_connection as check_datasource_connection_internal,
)
from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.project.datasource_discovery import DatasourceId
from databao_context_engine.project.layout import ensure_project_dir
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

    def build_context(
        self, datasource_ids: list[DatasourceId] | None, chunk_embedding_mode: ChunkEmbeddingMode
    ) -> list[BuildContextResult]:
        # TODO: Filter which datasources to build by datasource_ids
        return build_all_datasources(project_dir=self.project_dir, chunk_embedding_mode=chunk_embedding_mode)

    def check_datasource_connection(
        self, datasource_ids: list[DatasourceId] | None
    ) -> list[CheckDatasourceConnectionResult]:
        return sorted(
            check_datasource_connection_internal(project_dir=self.project_dir, datasource_ids=datasource_ids).values(),
            key=lambda result: str(result.datasource_id),
        )

    def create_datasource_config(
        self, datasource_type: DatasourceType, datasource_name: str, config_content: dict[str, Any]
    ) -> DatasourceConfigFile:
        # TODO: Before creating the datasource, validate the config content based on which plugin will be used
        config_file_path = create_datasource_config_file(
            project_dir=self.project_dir,
            datasource_type=datasource_type,
            datasource_name=datasource_name,
            config_content=config_content,
        )

        return DatasourceConfigFile(
            datasource_id=DatasourceId.from_datasource_config_file_path(config_file_path),
            config_file_path=config_file_path,
        )
