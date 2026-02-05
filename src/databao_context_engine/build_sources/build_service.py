from __future__ import annotations

import logging
from datetime import datetime

import cattrs
import yaml

from databao_context_engine.build_sources.plugin_execution import BuiltDatasourceContext, execute
from databao_context_engine.datasources.datasource_context import DatasourceContext
from databao_context_engine.datasources.types import PreparedDatasource
from databao_context_engine.pluginlib.build_plugin import (
    BuildPlugin,
)
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
        )

        return result

    def index_built_context(self, *, context: DatasourceContext, plugin: BuildPlugin) -> None:
        """Index a context file using the given plugin.

        1) Parses the yaml context file contents
        2) Reconstructs the `BuiltDatasourceContext` object
        3) Structures the inner `context` payload into the plugin's expected `context_type`
        4) Calls the plugin's chunker and persists the resulting chunks and embeddings.
        """
        raw_context = yaml.safe_load(context.context)

        converter = cattrs.Converter()
        converter.register_structure_hook(datetime, lambda v, _: v)
        build_datasource_context = converter.structure(raw_context, BuiltDatasourceContext)

        context_type = plugin.context_type
        typed_context = converter.structure(build_datasource_context.context, context_type)

        chunks = plugin.divide_context_into_chunks(typed_context)
        if not chunks:
            logger.info(
                "No chunks for %s — skipping indexing.", context.datasource_id.relative_path_to_context_file().name
            )
            return

        self._chunk_embedding_service.embed_chunks(
            chunks=chunks,
            result=context.context,
            full_type=build_datasource_context.datasource_type,
            datasource_id=build_datasource_context.datasource_id,
            override=True,
        )
