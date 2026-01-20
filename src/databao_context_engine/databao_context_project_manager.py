from pathlib import Path

from databao_context_engine.build_sources.public.api import BuildContextResult, build_all_datasources
from databao_context_engine.datasource_config.validate_config import (
    CheckDatasourceConnectionResult,
    validate_datasource_config,
)
from databao_context_engine.project.datasource_discovery import DatasourceId
from databao_context_engine.services.chunk_embedding_service import ChunkEmbeddingMode


class DatabaoContextProjectManager:
    project_dir: Path

    def __init__(self, project_dir: Path) -> None:
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
            validate_datasource_config(project_dir=self.project_dir, datasource_ids=datasource_ids).values(),
            key=lambda result: str(result.datasource_id),
        )
