from __future__ import annotations

import logging
from dataclasses import replace
from typing import Any

import yaml
from pydantic import BaseModel, TypeAdapter

import databao_context_engine.perf.core as perf
from databao_context_engine.build_sources.plugin_execution import BuiltDatasourceContext, execute_plugin
from databao_context_engine.datasources.datasource_context import (
    DatasourceContext,
    DatasourceContextHash,
    get_datasource_context,
    read_datasource_type_from_context,
)
from databao_context_engine.datasources.types import DatasourceId, PreparedDatasource
from databao_context_engine.llm.descriptions.provider import DescriptionProvider
from databao_context_engine.pluginlib.build_plugin import (
    BuildPlugin,
    DatasourceType,
)
from databao_context_engine.plugins.plugin_loader import DatabaoContextPluginLoader, NoPluginFoundForDatasource
from databao_context_engine.progress.progress import ProgressCallback, ProgressEmitter, ProgressStep
from databao_context_engine.project.layout import ProjectLayout
from databao_context_engine.services.chunk_embedding_service import ChunkEmbeddingService

logger = logging.getLogger(__name__)


class BuildService:
    def __init__(
        self,
        *,
        project_layout: ProjectLayout,
        chunk_embedding_service: ChunkEmbeddingService,
        plugin_loader: DatabaoContextPluginLoader,
        description_provider: DescriptionProvider | None = None,
    ) -> None:
        self._project_layout = project_layout
        self._chunk_embedding_service = chunk_embedding_service
        self._plugin_loader = plugin_loader
        self._description_provider = description_provider

    def build_context(
        self,
        *,
        prepared_source: PreparedDatasource,
        progress: ProgressCallback | None = None,
    ) -> BuiltDatasourceContext:
        """Process a single source to build its context.

        Returns:
            The built context.
        """
        emitter = ProgressEmitter(progress)

        plugin = self._get_plugin_for_datasource_type(prepared_source.datasource_type)
        result = self._execute_plugin(prepared_source=prepared_source, plugin=plugin)

        emitter.datasource_step_completed(
            datasource_id=result.datasource_id,
            step=ProgressStep.PLUGIN_EXECUTION,
        )

        return result

    @perf.perf_span("plugin.execute")
    def _execute_plugin(self, *, prepared_source: PreparedDatasource, plugin: BuildPlugin) -> BuiltDatasourceContext:
        return execute_plugin(self._project_layout, prepared_source, plugin)

    def index_datasource_context(
        self,
        *,
        context: DatasourceContext,
        force_index: bool = False,
        progress: ProgressCallback | None = None,
    ) -> None:
        """Index a context file using the given plugin.

        1) Reconstructs the `BuiltDatasourceContext` object from the yaml context string
        2) Calls the plugin's chunker and persists the resulting chunks and embeddings.
        """
        plugin = self._get_plugin_for_context(context=context)

        built = self._deserialize_built_context(context=context, context_type=plugin.context_type)

        self._index_built_context(
            built_context=built,
            context_hash=context.context_hash,
            plugin=plugin,
            force_index=force_index,
            progress=progress,
        )

    def index_built_context(
        self,
        *,
        built_context: BuiltDatasourceContext,
        context_hash: DatasourceContextHash,
        force_index: bool = False,
        progress: ProgressCallback | None = None,
    ) -> None:
        plugin = self._get_plugin_for_datasource_type(
            datasource_type=DatasourceType(full_type=built_context.datasource_type)
        )

        self._index_built_context(
            built_context=built_context,
            context_hash=context_hash,
            plugin=plugin,
            force_index=force_index,
            progress=progress,
        )

    def _index_built_context(
        self,
        *,
        built_context: BuiltDatasourceContext,
        context_hash: DatasourceContextHash,
        plugin: BuildPlugin,
        force_index: bool = False,
        progress: ProgressCallback | None,
    ) -> None:
        if not force_index and self._chunk_embedding_service.is_context_already_indexed(context_hash=context_hash):
            logger.info(f"Context for {str(context_hash.datasource_id)} has already been indexed, skipping indexing.")
            # Make sure to emit all step completed events
            BuildService.emit_all_index_step_as_completed(progress=progress, datasource_id=built_context.datasource_id)
            return

        perf.set_attribute("datasource_type", built_context.datasource_type)

        chunks = plugin.divide_context_into_chunks(built_context.context)
        perf.set_attribute("chunk_count", len(chunks))

        if not chunks:
            logger.info("No chunks for %s — skipping indexing.", built_context.datasource_id)
            return

        self._chunk_embedding_service.embed_chunks(
            chunks=chunks,
            context_hash=context_hash,
            full_type=built_context.datasource_type,
            datasource_id=built_context.datasource_id,
            override=force_index,
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

    def enrich_datasource_context(
        self, context: DatasourceContext, progress: ProgressCallback | None = None
    ) -> BuiltDatasourceContext:
        plugin = self._get_plugin_for_context(context=context)

        built = self._deserialize_built_context(context=context, context_type=plugin.context_type)

        return self._enrich_built_context(built_context=built, plugin=plugin, progress=progress)

    @perf.perf_span("plugin.enrich_context")
    def enrich_built_context(
        self, built_context: BuiltDatasourceContext, progress: ProgressCallback | None = None
    ) -> BuiltDatasourceContext:
        plugin = self._get_plugin_for_datasource_type(
            datasource_type=DatasourceType(full_type=built_context.datasource_type)
        )

        return self._enrich_built_context(built_context=built_context, plugin=plugin, progress=progress)

    def _enrich_built_context(
        self, built_context: BuiltDatasourceContext, plugin: BuildPlugin, progress: ProgressCallback | None = None
    ) -> BuiltDatasourceContext:
        if not self._description_provider:
            raise ValueError("Prompt provider should never be None when enrich_context is enabled")

        emitter = ProgressEmitter(progress)
        perf.set_attribute("datasource_type", built_context.datasource_type)

        new_context = plugin.enrich_context(built_context.context, self._description_provider)

        result = replace(built_context, context=new_context)

        emitter.datasource_step_completed(
            datasource_id=result.datasource_id,
            step=ProgressStep.CONTEXT_ENRICHMENT,
        )

        return result

    def index_context_if_necessary(self, datasource_context_hashes: list[DatasourceContextHash]) -> None:
        for datasource_context_hash in datasource_context_hashes:
            if not self._chunk_embedding_service.is_context_already_indexed(context_hash=datasource_context_hash):
                logger.info(
                    f"Index is missing for the current context of datasource {str(datasource_context_hash.datasource_id)}, it will be re-indexed."
                )

                context = get_datasource_context(self._project_layout, datasource_context_hash.datasource_id)

                self.index_datasource_context(
                    context=context,
                    # Forcing the index prevents checking for the datasource context hash again since we just did
                    force_index=True,
                )

    def _get_plugin_for_context(self, context: DatasourceContext) -> BuildPlugin:
        datasource_type = read_datasource_type_from_context(context)

        return self._get_plugin_for_datasource_type(datasource_type=datasource_type)

    def _get_plugin_for_datasource_type(self, datasource_type: DatasourceType) -> BuildPlugin:
        plugin = self._plugin_loader.get_plugin_for_datasource_type(datasource_type)
        if plugin is None:
            raise NoPluginFoundForDatasource(
                f"No plugin found for datasource type {datasource_type.full_type}", datasource_type=datasource_type
            )

        return plugin

    @staticmethod
    def build_context_step_plan() -> tuple[ProgressStep, ...]:
        return (ProgressStep.PLUGIN_EXECUTION,)

    @staticmethod
    def enrich_context_step_plan() -> tuple[ProgressStep, ...]:
        return (ProgressStep.CONTEXT_ENRICHMENT,)

    @staticmethod
    def index_step_plan() -> tuple[ProgressStep, ...]:
        return (
            ProgressStep.EMBEDDING,
            ProgressStep.PERSISTENCE,
        )

    @staticmethod
    def emit_all_index_step_as_completed(progress: ProgressCallback | None, datasource_id: str | DatasourceId) -> None:
        emitter = ProgressEmitter(progress)
        for step in BuildService.index_step_plan():
            emitter.datasource_step_completed(
                datasource_id=str(datasource_id),
                step=step,
            )
