import os

from duckdb import DuckDBPyConnection

from databao_context_engine.datasources.types import DatasourceId
from databao_context_engine.llm.embeddings.provider import EmbeddingProvider
from databao_context_engine.llm.factory import (
    create_ollama_embedding_provider,
    create_ollama_prompt_provider,
    create_ollama_service,
)
from databao_context_engine.llm.prompts.provider import PromptProvider
from databao_context_engine.project.layout import ProjectLayout
from databao_context_engine.retrieve_embeddings.retrieve_runner import retrieve
from databao_context_engine.retrieve_embeddings.retrieve_service import RAG_MODE, ContextSearchMode, RetrieveService
from databao_context_engine.services.factories import create_shard_resolver
from databao_context_engine.storage.connection import open_duckdb_connection
from databao_context_engine.storage.repositories.chunk_search_repository import SearchResult
from databao_context_engine.storage.repositories.factories import create_chunk_search_repository
from databao_context_engine.system.properties import get_db_path


def retrieve_embeddings(
    project_layout: ProjectLayout,
    retrieve_text: str,
    limit: int | None,
    datasource_ids: list[DatasourceId] | None,
    context_search_mode: ContextSearchMode,
    ollama_model_id: str | None = None,
    ollama_model_dim: int | None = None,
) -> list[SearchResult]:
    with open_duckdb_connection(get_db_path(project_layout.project_dir)) as conn:
        ollama_service = create_ollama_service()
        embedding_provider = create_ollama_embedding_provider(
            ollama_service, model_id=ollama_model_id, dim=ollama_model_dim
        )
        rag_mode = _get_rag_mode()
        prompt_provider = create_ollama_prompt_provider(ollama_service) if rag_mode == RAG_MODE.REWRITE_QUERY else None

        retrieve_service = _create_retrieve_service(
            conn, embedding_provider=embedding_provider, prompt_provider=prompt_provider
        )
        return retrieve(
            retrieve_service=retrieve_service,
            text=retrieve_text,
            limit=limit,
            datasource_ids=datasource_ids,
            rag_mode=rag_mode,
            context_search_mode=context_search_mode,
        )


def _get_rag_mode() -> RAG_MODE:
    rag_mode_env_var = os.environ.get("DATABAO_CONTEXT_RAG_MODE")
    if rag_mode_env_var:
        try:
            return RAG_MODE(rag_mode_env_var)
        except ValueError:
            pass

    return RAG_MODE.RAW_QUERY


def _create_retrieve_service(
    conn: DuckDBPyConnection,
    *,
    embedding_provider: EmbeddingProvider,
    prompt_provider: PromptProvider | None,
) -> RetrieveService:
    chunk_search_repo = create_chunk_search_repository(conn)
    shard_resolver = create_shard_resolver(conn)

    return RetrieveService(
        chunk_search_repo=chunk_search_repo,
        shard_resolver=shard_resolver,
        embedding_provider=embedding_provider,
        prompt_provider=prompt_provider,
    )
