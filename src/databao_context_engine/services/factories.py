from duckdb import DuckDBPyConnection

from databao_context_engine.llm.descriptions.provider import DescriptionProvider
from databao_context_engine.llm.embeddings.provider import EmbeddingProvider
from databao_context_engine.services.chunk_embedding_service import ChunkEmbeddingMode, ChunkEmbeddingService
from databao_context_engine.services.embedding_shard_resolver import EmbeddingShardResolver
from databao_context_engine.services.persistence_service import PersistenceService
from databao_context_engine.services.table_name_policy import TableNamePolicy
from databao_context_engine.storage.repositories.factories import (
    create_chunk_repository,
    create_embedding_repository,
    create_registry_repository,
)


def create_shard_resolver(conn: DuckDBPyConnection, policy: TableNamePolicy | None = None) -> EmbeddingShardResolver:
    return EmbeddingShardResolver(
        conn=conn, registry_repo=create_registry_repository(conn), table_name_policy=policy or TableNamePolicy()
    )


def create_persistence_service(conn: DuckDBPyConnection, *, model_dim: int | None) -> PersistenceService:
    return PersistenceService(
        conn=conn,
        chunk_repo=create_chunk_repository(conn),
        embedding_repo=create_embedding_repository(conn),
        dim=model_dim,
    )


def create_chunk_embedding_service(
    conn: DuckDBPyConnection,
    *,
    embedding_provider: EmbeddingProvider,
    description_provider: DescriptionProvider | None,
    chunk_embedding_mode: ChunkEmbeddingMode,
) -> ChunkEmbeddingService:
    resolver = create_shard_resolver(conn)
    persistence = create_persistence_service(conn, model_dim=embedding_provider.dim)
    return ChunkEmbeddingService(
        persistence_service=persistence,
        embedding_provider=embedding_provider,
        shard_resolver=resolver,
        description_provider=description_provider,
        chunk_embedding_mode=chunk_embedding_mode,
    )
