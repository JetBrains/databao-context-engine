from __future__ import annotations

import logging
from dataclasses import replace
from typing import Any

import yaml
from pydantic import BaseModel, TypeAdapter

import databao_context_engine.perf.core as perf
from databao_context_engine.build_sources.plugin_execution import BuiltDatasourceContext, execute_plugin
from databao_context_engine.datasources.datasource_context import DatasourceContext
from databao_context_engine.datasources.types import PreparedDatasource
from databao_context_engine.pluginlib.build_plugin import (
    BuildPlugin,
)
from databao_context_engine.project.layout import ProjectLayout
from databao_context_engine.services.chunk_embedding_service import ChunkEmbeddingService

logger = logging.getLogger(__name__)


class BuildService:
    def __init__(
        self,
        *,
        project_layout: ProjectLayout,
        chunk_embedding_service: ChunkEmbeddingService,
    ) -> None:
        self._project_layout = project_layout
        self._chunk_embedding_service = chunk_embedding_service

    def process_prepared_source(
        self, *, prepared_source: PreparedDatasource, plugin: BuildPlugin, generate_embeddings: bool = True
    ) -> BuiltDatasourceContext:
        """Process a single source to build its context.

        1) Execute the plugin
        2) Divide the results into chunks
        3) Embed and persist the chunks

        Returns:
            The built context.
        """
        result = self._execute_plugin(prepared_source=prepared_source, plugin=plugin)

        if not generate_embeddings:
            return result

        chunks = plugin.divide_context_into_chunks(result.context)

        perf.set_attribute("chunk_count", len(chunks))

        if not chunks:
            logger.info("No chunks for %s — skipping.", prepared_source.datasource_id.relative_path_to_config_file())
            return result

        self._chunk_embedding_service.embed_chunks(
            chunks=chunks,
            result=result,
            full_type=prepared_source.datasource_type.full_type,
            datasource_id=result.datasource_id,
        )

        return result

    @perf.perf_span("plugin.execute")
    def _execute_plugin(self, *, prepared_source: PreparedDatasource, plugin: BuildPlugin) -> BuiltDatasourceContext:
        return execute_plugin(self._project_layout, prepared_source, plugin)

    def index_built_context(self, *, context: DatasourceContext, plugin: BuildPlugin) -> None:
        """Index a context file using the given plugin.

        1) Parses the yaml context file contents
        2) Reconstructs the `BuiltDatasourceContext` object
        3) Structures the inner `context` payload into the plugin's expected `context_type`
        4) Calls the plugin's chunker and persists the resulting chunks and embeddings.
        """
        built = self._deserialize_built_context(context=context, context_type=plugin.context_type)

        chunks = plugin.divide_context_into_chunks(built.context)
        perf.set_attribute("chunk_count", len(chunks))

        if not chunks:
            logger.info(
                "No chunks for %s — skipping indexing.", context.datasource_id.relative_path_to_context_file().name
            )
            return

        self._chunk_embedding_service.embed_chunks(
            chunks=chunks,
            result=built,
            full_type=built.datasource_type,
            datasource_id=built.datasource_id,
            override=True,
        )

    def _deserialize_built_context(
        self,
        *,
        context: DatasourceContext,
        context_type: type[Any],
    ) -> BuiltDatasourceContext:
        """Parse the YAML payload and return a BuiltDatasourceContext with a typed `.context`."""
        raw_context = yaml.safe_load(context.context)

        built = TypeAdapter(BuiltDatasourceContext).validate_python(raw_context)

        if isinstance(context_type, type) and issubclass(context_type, BaseModel):
            typed_context: Any = context_type.model_validate(built.context)
        else:
            typed_context = TypeAdapter(context_type).validate_python(built.context)

        return replace(built, context=typed_context)
