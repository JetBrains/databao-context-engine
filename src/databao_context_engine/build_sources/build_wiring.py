import logging

from duckdb import DuckDBPyConnection

from databao_context_engine.build_sources.build_runner import BuildContextResult, build
from databao_context_engine.build_sources.build_service import BuildService
from databao_context_engine.llm.descriptions.provider import DescriptionProvider
from databao_context_engine.llm.embeddings.provider import EmbeddingProvider
from databao_context_engine.llm.factory import (
    create_ollama_description_provider,
    create_ollama_embedding_provider,
    create_ollama_service,
)
from databao_context_engine.progress.progress import ProgressCallback
from databao_context_engine.project.layout import ProjectLayout
from databao_context_engine.services.chunk_embedding_service import ChunkEmbeddingMode
from databao_context_engine.services.factories import create_chunk_embedding_service
from databao_context_engine.storage.connection import open_duckdb_connection
from databao_context_engine.storage.migrate import migrate
from databao_context_engine.system.properties import get_db_path

logger = logging.getLogger(__name__)


def build_all_datasources(
    project_layout: ProjectLayout,
    chunk_embedding_mode: ChunkEmbeddingMode,
    *,
    progress: ProgressCallback | None = None,
) -> list[BuildContextResult]:
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
    db_path = get_db_path(project_layout.project_dir)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    migrate(db_path)
    with open_duckdb_connection(db_path) as conn:
        ollama_service = create_ollama_service()
        embedding_provider = create_ollama_embedding_provider(ollama_service)
        description_provider = (
            create_ollama_description_provider(ollama_service)
            if chunk_embedding_mode.should_generate_description()
            else None
        )
        build_service = _create_build_service(
            conn,
            project_layout=project_layout,
            embedding_provider=embedding_provider,
            description_provider=description_provider,
            chunk_embedding_mode=chunk_embedding_mode,
        )
        return build(
            project_layout=project_layout,
            build_service=build_service,
            progress=progress,
        )


def _create_build_service(
    conn: DuckDBPyConnection,
    *,
    project_layout: ProjectLayout,
    embedding_provider: EmbeddingProvider,
    description_provider: DescriptionProvider | None,
    chunk_embedding_mode: ChunkEmbeddingMode,
) -> BuildService:
    chunk_embedding_service = create_chunk_embedding_service(
        conn,
        embedding_provider=embedding_provider,
        description_provider=description_provider,
        chunk_embedding_mode=chunk_embedding_mode,
    )

    return BuildService(
        project_layout=project_layout,
        chunk_embedding_service=chunk_embedding_service,
    )
