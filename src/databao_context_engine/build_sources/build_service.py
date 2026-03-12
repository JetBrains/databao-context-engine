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
from databao_context_engine.llm.descriptions.provider import DescriptionProvider
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
        description_provider: DescriptionProvider | None = None,
    ) -> None:
        self._project_layout = project_layout
        self._chunk_embedding_service = chunk_embedding_service
        self._description_provider = description_provider

    def build_context(
        self,
        *,
        prepared_source: PreparedDatasource,
        plugin: BuildPlugin,
    ) -> BuiltDatasourceContext:
        """Process a single source to build its context.

        Returns:
            The built context.
        """
        return self._execute_plugin(prepared_source=prepared_source, plugin=plugin)

    @perf.perf_span("plugin.execute")
    def _execute_plugin(self, *, prepared_source: PreparedDatasource, plugin: BuildPlugin) -> BuiltDatasourceContext:
        return execute_plugin(self._project_layout, prepared_source, plugin)

    def index_datasource_context(self, *, context: DatasourceContext, plugin: BuildPlugin) -> None:
        """Index a context file using the given plugin.

        1) Reconstructs the `BuiltDatasourceContext` object from the yaml context string
        2) Calls the plugin's chunker and persists the resulting chunks and embeddings.
        """
        built = self._deserialize_built_context(context=context, context_type=plugin.context_type)

        self.index_built_context(built_context=built, plugin=plugin, override=True)

    def index_built_context(
        self, *, built_context: BuiltDatasourceContext, plugin: BuildPlugin, override: bool = False
    ) -> None:
        chunks = plugin.divide_context_into_chunks(built_context.context)
        perf.set_attribute("chunk_count", len(chunks))

        if not chunks:
            logger.info("No chunks for %s — skipping indexing.", built_context.datasource_id)
            return

        self._chunk_embedding_service.embed_chunks(
            chunks=chunks,
            result=built_context,
            full_type=built_context.datasource_type,
            datasource_id=built_context.datasource_id,
            override=override,
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

    def enrich_datasource_context(self, context: DatasourceContext, plugin: BuildPlugin) -> BuiltDatasourceContext:
        built = self._deserialize_built_context(context=context, context_type=plugin.context_type)

        return self.enrich_built_context(built_context=built, plugin=plugin)

    @perf.perf_span("plugin.enrich_context")
    def enrich_built_context(
        self, built_context: BuiltDatasourceContext, plugin: BuildPlugin
    ) -> BuiltDatasourceContext:
        if not self._description_provider:
            raise ValueError("Prompt provider should never be None when enrich_context is enabled")

        new_context = plugin.enrich_context(built_context.context, self._description_provider)

        return replace(built_context, context=new_context)
