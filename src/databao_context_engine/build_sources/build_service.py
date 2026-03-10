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
from databao_context_engine.progress.progress import ProgressCallback, ProgressEmitter
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

    def build_context(
        self,
        *,
        prepared_source: PreparedDatasource,
        plugin: BuildPlugin,
        should_index: bool,
        progress: ProgressCallback | None = None,
    ) -> BuiltDatasourceContext:
        """Process a single source to build its context.

        1) Execute the plugin
        2) Divide the results into chunks
        3) Embed and persist the chunks

        Returns:
            The built context.
        """
        result = self._execute_plugin(prepared_source=prepared_source, plugin=plugin)

        emitter = ProgressEmitter(progress)

        if not should_index:
            emitter.datasource_total_steps_set(datasource_id=result.datasource_id, total_steps=1)
            emitter.datasource_step_completed(datasource_id=result.datasource_id)
            return result

        self._index_context(
            built_context=result,
            plugin=plugin,
            progress=progress,
            include_plugin_execution_step=True,
        )

        return result

    @perf.perf_span("plugin.execute")
    def _execute_plugin(self, *, prepared_source: PreparedDatasource, plugin: BuildPlugin) -> BuiltDatasourceContext:
        return execute_plugin(self._project_layout, prepared_source, plugin)

    def index_built_context(
        self,
        *,
        context: DatasourceContext,
        plugin: BuildPlugin,
        progress: ProgressCallback | None = None,
    ) -> None:
        """Index a context file using the given plugin.

        1) Parses the yaml context file contents
        2) Reconstructs the `BuiltDatasourceContext` object
        3) Structures the inner `context` payload into the plugin's expected `context_type`
        4) Calls the plugin's chunker and persists the resulting chunks and embeddings.
        """
        built = self._deserialize_built_context(context=context, context_type=plugin.context_type)

        self._index_context(
            built_context=built,
            plugin=plugin,
            override=True,
            progress=progress,
            include_plugin_execution_step=False,
        )

    def _index_context(
        self,
        *,
        built_context: BuiltDatasourceContext,
        plugin: BuildPlugin,
        override: bool = False,
        progress: ProgressCallback | None = None,
        include_plugin_execution_step: bool,
    ) -> None:
        chunks = plugin.divide_context_into_chunks(built_context.context)
        perf.set_attribute("chunk_count", len(chunks))

        emitter = ProgressEmitter(progress)

        if not chunks:
            logger.info("No chunks for %s — skipping indexing.", built_context.datasource_id)
            if include_plugin_execution_step:
                emitter.datasource_total_steps_set(datasource_id=built_context.datasource_id, total_steps=1)
                emitter.datasource_step_completed(datasource_id=built_context.datasource_id)
            return

        total_steps = (1 if include_plugin_execution_step else 0) + self._chunk_embedding_service.indexing_step_count()

        emitter.datasource_total_steps_set(
            datasource_id=built_context.datasource_id,
            total_steps=total_steps,
        )

        if include_plugin_execution_step:
            emitter.datasource_step_completed(datasource_id=built_context.datasource_id)

        self._chunk_embedding_service.embed_chunks(
            chunks=chunks,
            result=built_context,
            full_type=built_context.datasource_type,
            datasource_id=built_context.datasource_id,
            override=override,
            progress=progress,
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
