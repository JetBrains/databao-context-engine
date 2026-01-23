from databao_context_engine.llm.factory import create_ollama_embedding_provider, create_ollama_service
from databao_context_engine.project.layout import ProjectLayout, ensure_project_dir
from databao_context_engine.retrieve_embeddings.internal.retrieve_runner import retrieve
from databao_context_engine.services.factories import create_retrieve_service
from databao_context_engine.storage.connection import open_duckdb_connection
from databao_context_engine.storage.repositories.vector_search_repository import VectorSearchResult
from databao_context_engine.system.properties import get_db_path


def retrieve_embeddings(
    project_layout: ProjectLayout,
    retrieve_text: str,
    limit: int | None,
    export_to_file: bool,
) -> list[VectorSearchResult]:
    ensure_project_dir(project_layout.project_dir)

    with open_duckdb_connection(get_db_path(project_layout.project_dir)) as conn:
        ollama_service = create_ollama_service()
        embedding_provider = create_ollama_embedding_provider(ollama_service)
        retrieve_service = create_retrieve_service(conn, embedding_provider=embedding_provider)
        return retrieve(
            project_dir=project_layout.project_dir,
            retrieve_service=retrieve_service,
            project_id=str(project_layout.read_config_file().project_id),
            text=retrieve_text,
            limit=limit,
            export_to_file=export_to_file,
        )
