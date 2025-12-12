import logging
from pathlib import Path

from nemory.build_sources.internal.build_runner import build
from nemory.llm.factory import (
    create_ollama_description_provider,
    create_ollama_service,
    create_ollama_embedding_provider,
)
from nemory.project.info import get_nemory_version
from nemory.project.layout import ensure_project_dir, read_config_file
from nemory.services.chunk_embedding_service import ChunkEmbeddingMode
from nemory.services.factories import (
    create_build_service,
)
from nemory.storage.connection import open_duckdb_connection
from nemory.system.properties import get_db_path

logger = logging.getLogger(__name__)


def build_all_datasources(project_dir: Path, chunk_embedding_mode: ChunkEmbeddingMode):
    """
    Public build entrypoint
    - Instantiates the build service
    - Delegates the actual build logic to the build runner
    """
    ensure_project_dir(project_dir)

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
        nemory_config = read_config_file(project_dir)
        build(
            project_dir=project_dir,
            build_service=build_service,
            project_id=str(nemory_config.project_id),
            nemory_version=get_nemory_version(),
        )
