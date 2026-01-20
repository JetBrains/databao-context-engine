import logging
from pathlib import Path

from databao_context_engine.build_sources.internal.build_runner import BuildContextResult, build
from databao_context_engine.llm.factory import (
    create_ollama_description_provider,
    create_ollama_embedding_provider,
    create_ollama_service,
)
from databao_context_engine.project.info import get_dce_version
from databao_context_engine.project.layout import ensure_project_dir
from databao_context_engine.services.chunk_embedding_service import ChunkEmbeddingMode
from databao_context_engine.services.factories import (
    create_build_service,
)
from databao_context_engine.storage.connection import open_duckdb_connection
from databao_context_engine.system.properties import get_db_path

logger = logging.getLogger(__name__)


def build_all_datasources(project_dir: Path, chunk_embedding_mode: ChunkEmbeddingMode) -> list[BuildContextResult]:
    """
    Public build entrypoint
    - Instantiates the build service
    - Delegates the actual build logic to the build runner
    """
    project_layout = ensure_project_dir(project_dir)

    logger.debug(f"Starting to build datasources in project {project_dir.resolve()}")

    with open_duckdb_connection(get_db_path()) as conn:
        ollama_service = create_ollama_service()
        embedding_provider = create_ollama_embedding_provider(ollama_service)
        description_provider = (
            create_ollama_description_provider(ollama_service)
            if chunk_embedding_mode.should_generate_description()
            else None
        )
        build_service = create_build_service(
            conn,
            embedding_provider=embedding_provider,
            description_provider=description_provider,
            chunk_embedding_mode=chunk_embedding_mode,
        )
        dce_config = project_layout.read_config_file()
        return build(
            project_dir=project_dir,
            build_service=build_service,
            project_id=str(dce_config.project_id),
            dce_version=get_dce_version(),
        )
