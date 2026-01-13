from __future__ import annotations

import logging
from datetime import datetime

from nemory.build_sources.internal.plugin_execution import execute
from nemory.datasource_config.utils import get_datasource_id_from_type_and_name
from nemory.pluginlib.build_plugin import (
    BuildExecutionResult,
    BuildPlugin,
)
from nemory.project.types import PreparedDatasource
from nemory.services.chunk_embedding_service import ChunkEmbeddingService
from nemory.storage.models import RunDTO
from nemory.storage.repositories.datasource_run_repository import DatasourceRunRepository
from nemory.storage.repositories.run_repository import RunRepository

logger = logging.getLogger(__name__)


class BuildService:
    def __init__(
        self,
        *,
        run_repo: RunRepository,
        datasource_run_repo: DatasourceRunRepository,
        chunk_embedding_service: ChunkEmbeddingService,
    ) -> None:
        self._run_repo = run_repo
        self._datasource_run_repo = datasource_run_repo
        self._chunk_embedding_service = chunk_embedding_service

    def start_run(self, *, project_id: str, nemory_version: str) -> RunDTO:
        """
        Create a new run row and return (run_id, started_at).
        """
        return self._run_repo.create(project_id=project_id, nemory_version=nemory_version)

    def finalize_run(self, *, run_id: int):
        """
        Mark the run as complete (sets ended_at).
        """
        self._run_repo.update(run_id=run_id, ended_at=datetime.now())

    def process_prepared_source(
        self,
        *,
        run_id: int,
        prepared_source: PreparedDatasource,
        plugin: BuildPlugin,
    ) -> BuildExecutionResult:
        """
        Process a single source.

        1) Execute the plugin
        2) Divide the results into chunks
        3) Embed and persist the chunks
        """
        result = execute(prepared_source, plugin)

        chunks = plugin.divide_result_into_chunks(result)
        if not chunks:
            logger.info("No chunks for %s — skipping.", prepared_source.path.name)
            return result

        datasource_run = self._datasource_run_repo.create(
            run_id=run_id,
            plugin=plugin.name,
            full_type=prepared_source.datasource_type.full_type,
            source_id=get_datasource_id_from_type_and_name(
                prepared_source.datasource_type, prepared_source.datasource_name
            ),
            storage_directory=str(prepared_source.path.parent),
        )

        self._chunk_embedding_service.embed_chunks(
            datasource_run_id=datasource_run.datasource_run_id, chunks=chunks, result=repr(result.result)
        )

        return result
