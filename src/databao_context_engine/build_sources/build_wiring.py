import logging

from duckdb import DuckDBPyConnection

from databao_context_engine.build_sources.build_runner import (
    build,
    run_indexing,
)
from databao_context_engine.build_sources.build_service import BuildService
from databao_context_engine.build_sources.types import BuildDatasourceResult, IndexDatasourceResult
from databao_context_engine.datasources.datasource_context import DatasourceContext
from databao_context_engine.llm.factory import (
    create_ollama_description_provider,
    create_ollama_embedding_provider,
    create_ollama_service,
)
from databao_context_engine.plugins.plugin_loader import DatabaoContextPluginLoader
from databao_context_engine.project.layout import ProjectLayout
from databao_context_engine.services.chunk_embedding_service import ChunkEmbeddingMode
from databao_context_engine.services.factories import create_chunk_embedding_service
from databao_context_engine.storage.connection import open_duckdb_connection
from databao_context_engine.storage.migrate import migrate

logger = logging.getLogger(__name__)


def build_all_datasources(
    project_layout: ProjectLayout,
    plugin_loader: DatabaoContextPluginLoader,
    chunk_embedding_mode: ChunkEmbeddingMode,
    should_index: bool,
    should_enrich_context: bool,
) -> list[BuildDatasourceResult]:
    """Build the context for all datasources in the project.

    - Instantiates the build service
    - Delegates the actual build logic to the build runner

    Returns:
        A list of all the contexts built.
    """
    logger.debug(f"Starting to build datasources in project {project_layout.project_dir.resolve()}")

    # Think about alternative solutions. This solution will mirror the current behaviour
    # The current behaviour only builds what is currently in the /src folder
    # This will need to change in the future when we can pick which datasources to build
    db_path = project_layout.db_path
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    migrate(db_path)
    with open_duckdb_connection(db_path) as conn:
        build_service = _create_build_service(
            conn,
            project_layout=project_layout,
            chunk_embedding_mode=chunk_embedding_mode,
            should_enrich_context=should_enrich_context,
        )
        return build(
            project_layout=project_layout,
            plugin_loader=plugin_loader,
            build_service=build_service,
            should_index=should_index,
            should_enrich_context=should_enrich_context,
        )


def index_built_contexts(
    project_layout: ProjectLayout,
    plugin_loader: DatabaoContextPluginLoader,
    contexts: list[DatasourceContext],
    chunk_embedding_mode: ChunkEmbeddingMode,
) -> list[IndexDatasourceResult]:
    """Index the contexts into the database.

    - Instantiates the build service
    - If the database does not exist, it creates it.

    Returns:
        A list of all the contexts indexed.
    """
    logger.debug("Starting to index %d context(s) for project %s", len(contexts), project_layout.project_dir.resolve())

    db_path = project_layout.db_path
    if not db_path.exists():
        db_path.parent.mkdir(parents=True, exist_ok=True)
        migrate(db_path)

    with open_duckdb_connection(db_path) as conn:
        build_service = _create_build_service(
            conn,
            project_layout=project_layout,
            chunk_embedding_mode=chunk_embedding_mode,
            should_enrich_context=False,
        )
        return run_indexing(
            project_layout=project_layout, plugin_loader=plugin_loader, build_service=build_service, contexts=contexts
        )


def _create_build_service(
    conn: DuckDBPyConnection,
    *,
    project_layout: ProjectLayout,
    chunk_embedding_mode: ChunkEmbeddingMode,
    should_enrich_context: bool,
) -> BuildService:
    ollama_service = create_ollama_service()
    embedding_provider = create_ollama_embedding_provider(
        ollama_service, model_details=project_layout.project_config.ollama_embedding_model_details
    )
    description_provider = (
        create_ollama_description_provider(ollama_service)
        if chunk_embedding_mode.should_generate_description() or should_enrich_context
        else None
    )

    chunk_embedding_service = create_chunk_embedding_service(
        conn,
        embedding_provider=embedding_provider,
        description_provider=description_provider,
        chunk_embedding_mode=chunk_embedding_mode,
    )

    return BuildService(
        project_layout=project_layout,
        chunk_embedding_service=chunk_embedding_service,
        description_provider=description_provider,
    )
