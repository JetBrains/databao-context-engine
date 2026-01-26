from _duckdb import DuckDBPyConnection

from databao_context_engine.build_sources.build_service import BuildService
from databao_context_engine.llm.descriptions.provider import DescriptionProvider
from databao_context_engine.llm.embeddings.provider import EmbeddingProvider
from databao_context_engine.retrieve_embeddings.internal.retrieve_service import RetrieveService
from databao_context_engine.services.chunk_embedding_service import ChunkEmbeddingMode, ChunkEmbeddingService
from databao_context_engine.services.embedding_shard_resolver import EmbeddingShardResolver
from databao_context_engine.services.persistence_service import PersistenceService
from databao_context_engine.services.table_name_policy import TableNamePolicy
from databao_context_engine.storage.repositories.factories import (
    create_chunk_repository,
    create_embedding_repository,
    create_registry_repository,
    create_vector_search_repository,
)


def create_shard_resolver(conn: DuckDBPyConnection, policy: TableNamePolicy | None = None) -> EmbeddingShardResolver:
    return EmbeddingShardResolver(
        conn=conn, registry_repo=create_registry_repository(conn), table_name_policy=policy or TableNamePolicy()
    )


def create_persistence_service(conn: DuckDBPyConnection) -> PersistenceService:
    return PersistenceService(
        conn=conn, chunk_repo=create_chunk_repository(conn), embedding_repo=create_embedding_repository(conn)
    )


def create_chunk_embedding_service(
    conn: DuckDBPyConnection,
    *,
    embedding_provider: EmbeddingProvider,
    description_provider: DescriptionProvider | None,
    chunk_embedding_mode: ChunkEmbeddingMode,
) -> ChunkEmbeddingService:
    resolver = create_shard_resolver(conn)
    persistence = create_persistence_service(conn)
    return ChunkEmbeddingService(
        persistence_service=persistence,
        embedding_provider=embedding_provider,
        shard_resolver=resolver,
        description_provider=description_provider,
        chunk_embedding_mode=chunk_embedding_mode,
    )


def create_build_service(
    conn: DuckDBPyConnection,
    *,
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
        chunk_embedding_service=chunk_embedding_service,
    )


def create_retrieve_service(
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
