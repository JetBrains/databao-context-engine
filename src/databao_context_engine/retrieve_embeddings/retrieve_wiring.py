from duckdb import DuckDBPyConnection

from databao_context_engine.datasources.types import DatasourceId
from databao_context_engine.llm.embeddings.provider import EmbeddingProvider
from databao_context_engine.llm.factory import create_ollama_embedding_provider, create_ollama_service
from databao_context_engine.project.layout import ProjectLayout
from databao_context_engine.retrieve_embeddings.retrieve_runner import retrieve
from databao_context_engine.retrieve_embeddings.retrieve_service import RetrieveService
from databao_context_engine.services.factories import create_shard_resolver
from databao_context_engine.storage.connection import open_duckdb_connection
from databao_context_engine.storage.repositories.factories import create_vector_search_repository
from databao_context_engine.storage.repositories.vector_search_repository import VectorSearchResult
from databao_context_engine.system.properties import get_db_path


def retrieve_embeddings(
    project_layout: ProjectLayout,
    retrieve_text: str,
    limit: int | None,
    datasource_ids: list[DatasourceId] | None,
    ollama_model_id: str | None = None,
    ollama_model_dim: int | None = None,
) -> list[VectorSearchResult]:
    with open_duckdb_connection(get_db_path(project_layout.project_dir)) as conn:
        ollama_service = create_ollama_service()
        embedding_provider = create_ollama_embedding_provider(
            ollama_service, model_id=ollama_model_id, dim=ollama_model_dim
        )
        retrieve_service = _create_retrieve_service(conn, embedding_provider=embedding_provider)
        return retrieve(
            retrieve_service=retrieve_service,
            text=retrieve_text,
            limit=limit,
            datasource_ids=datasource_ids,
        )


def _create_retrieve_service(
    conn: DuckDBPyConnection,
    *,
    embedding_provider: EmbeddingProvider,
) -> RetrieveService:
    vector_search_repo = create_vector_search_repository(conn)
    shard_resolver = create_shard_resolver(conn)

    return RetrieveService(
        vector_search_repo=vector_search_repo,
        shard_resolver=shard_resolver,
        provider=embedding_provider,
    )
