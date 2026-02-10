from __future__ import annotations

import logging

from databao_context_engine.build_sources.plugin_execution import BuiltDatasourceContext, execute
from databao_context_engine.datasources.types import PreparedDatasource
from databao_context_engine.pluginlib.build_plugin import (
    BuildPlugin,
)
from databao_context_engine.progress.progress import ProgressCallback
from databao_context_engine.serialization.yaml import to_yaml_string
from databao_context_engine.services.chunk_embedding_service import ChunkEmbeddingService

logger = logging.getLogger(__name__)


class BuildService:
    def __init__(
        self,
        *,
        chunk_embedding_service: ChunkEmbeddingService,
    ) -> None:
        self._chunk_embedding_service = chunk_embedding_service

    def process_prepared_source(
        self,
        *,
        prepared_source: PreparedDatasource,
        plugin: BuildPlugin,
        progress: ProgressCallback | None = None,
    ) -> BuiltDatasourceContext:
        """Process a single source to build its context.

        1) Execute the plugin
        2) Divide the results into chunks
        3) Embed and persist the chunks

        Returns:
            The built context.
        """
        result = execute(prepared_source, plugin)

        chunks = plugin.divide_context_into_chunks(result.context)
        if not chunks:
            logger.info("No chunks for %s — skipping.", prepared_source.path.name)
            return result

        self._chunk_embedding_service.embed_chunks(
            chunks=chunks,
            result=to_yaml_string(result.context),
            full_type=prepared_source.datasource_type.full_type,
            datasource_id=result.datasource_id,
            progress=progress,
        )

        return result
