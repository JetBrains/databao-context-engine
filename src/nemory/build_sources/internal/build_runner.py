import logging
from pathlib import Path
from datetime import datetime
from nemory.build_sources.internal.source_discovery import discover_sources, full_type_of
from nemory.build_sources.internal.plugin_execution import divide_into_chunks, execute
from nemory.build_sources.internal.export_results import (
    export_build_result,
    create_run_dir,
    append_result_to_all_results,
)
from nemory.build_sources.internal.plugin_loader import load_plugins
from nemory.pluginlib.build_plugin import BuildPlugin
from nemory.services.chunk_embedding_service import ChunkEmbeddingService
from nemory.storage.repositories.datasource_run_repository import DatasourceRunRepository
from nemory.storage.repositories.run_repository import RunRepository

logger = logging.getLogger(__name__)


def build(
    project_dir: Path,
    *,
    chunk_embedding_service: ChunkEmbeddingService,
    run_repo: RunRepository,
    datasource_run_repo: DatasourceRunRepository,
    project_id: str,
    nemory_version: str | None = None,
) -> None:
    """
    Build entrypoint.

    1) Load available plugins
    2) Discover sources
    3) Create a run
    4) For each source, call process_source
    """
    plugins = load_plugins()
    sources = discover_sources(project_dir)
    if not sources:
        logger.info("No sources discovered under %s", project_dir)
        return

    run = run_repo.create(project_id=project_id, nemory_version=nemory_version)
    run_dir = create_run_dir(project_dir, run.started_at)
    for source in sources:
        try:
            full_type = full_type_of(source)
            plugin = plugins.get(full_type)
            if plugin is None:
                logger.warning("No plugin for '%s' (source=%s) — skipping.", full_type, source.path)
                continue
            process_source(
                source=source,
                plugin=plugin,
                datasource_run_repo=datasource_run_repo,
                run_id=run.run_id,
                run_dir=run_dir,
                chunk_embedding_service=chunk_embedding_service,
            )
        except Exception as e:
            logger.exception("Source failed (%s): %s", source.path, e)

    run_repo.update(run.run_id, ended_at=datetime.now())

    logger.info("Build complete. Processed %d sources.", len(sources))


def process_source(
    *,
    source,
    plugin: BuildPlugin,
    datasource_run_repo: DatasourceRunRepository,
    run_id: int,
    run_dir: Path,
    chunk_embedding_service: ChunkEmbeddingService,
):
    """
    Process a single source.

    1) Resolve its full type
    2) Execute the plugin
    3) Divide the results into chunks
    4) Embed and persist the chunks
    """
    full_type = full_type_of(source)

    result = execute(source, plugin)

    export_build_result(run_dir, result)
    append_result_to_all_results(run_dir, result)

    chunks = divide_into_chunks(plugin, result)
    if not chunks:
        logger.info("No chunks for %s (%s) — skipping.", source.path.name, full_type)
        return

    datasource_run = datasource_run_repo.create(
        run_id=run_id,
        plugin=plugin.name,
        source_id=(result.id or source.path.stem),
        storage_directory=str(source.path.parent),
    )

    chunk_embedding_service.embed_chunks(datasource_run_id=datasource_run.datasource_run_id, chunks=chunks)
