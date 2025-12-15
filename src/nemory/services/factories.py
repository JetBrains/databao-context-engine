from _duckdb import DuckDBPyConnection

from nemory.build_sources.internal.build_service import BuildService
from nemory.llm.descriptions.provider import DescriptionProvider
from nemory.llm.embeddings.provider import EmbeddingProvider
from nemory.retrieve_embeddings.internal.retrieve_service import RetrieveService
from nemory.services.chunk_embedding_service import ChunkEmbeddingMode, ChunkEmbeddingService
from nemory.services.embedding_shard_resolver import EmbeddingShardResolver
from nemory.services.persistence_service import PersistenceService
from nemory.services.run_name_policy import RunNamePolicy
from nemory.services.table_name_policy import TableNamePolicy
from nemory.storage.repositories.chunk_repository import ChunkRepository
from nemory.storage.repositories.datasource_run_repository import DatasourceRunRepository
from nemory.storage.repositories.embedding_model_registry_repository import EmbeddingModelRegistryRepository
from nemory.storage.repositories.embedding_repository import EmbeddingRepository
from nemory.storage.repositories.run_repository import RunRepository
from nemory.storage.repositories.vector_search_repository import VectorSearchRepository


def create_run_repository(conn: DuckDBPyConnection) -> RunRepository:
    return RunRepository(conn, run_name_policy=RunNamePolicy())


def create_datasource_run_repository(conn: DuckDBPyConnection) -> DatasourceRunRepository:
    return DatasourceRunRepository(conn)


def create_chunk_repository(conn: DuckDBPyConnection) -> ChunkRepository:
    return ChunkRepository(conn)


def create_embedding_repository(conn: DuckDBPyConnection) -> EmbeddingRepository:
    return EmbeddingRepository(conn)


def create_registry_repository(conn: DuckDBPyConnection) -> EmbeddingModelRegistryRepository:
    return EmbeddingModelRegistryRepository(conn)


def create_vector_search_repository(conn: DuckDBPyConnection) -> VectorSearchRepository:
    return VectorSearchRepository(conn)


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
    run_repo = create_run_repository(conn)
    datasource_run_repo = create_datasource_run_repository(conn)
    chunk_embedding_service = create_chunk_embedding_service(
        conn,
        embedding_provider=embedding_provider,
        description_provider=description_provider,
        chunk_embedding_mode=chunk_embedding_mode,
    )

    return BuildService(
        run_repo=run_repo,
        datasource_run_repo=datasource_run_repo,
        chunk_embedding_service=chunk_embedding_service,
    )


def create_retrieve_service(
    conn: DuckDBPyConnection,
    *,
    embedding_provider: EmbeddingProvider,
) -> RetrieveService:
    run_repo = create_run_repository(conn)
    vector_search_repo = create_vector_search_repository(conn)
    shard_resolver = create_shard_resolver(conn)

    return RetrieveService(
        run_repo=run_repo,
        vector_search_repo=vector_search_repo,
        shard_resolver=shard_resolver,
        provider=embedding_provider,
    )
