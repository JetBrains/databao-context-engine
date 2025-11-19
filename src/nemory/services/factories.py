from _duckdb import DuckDBPyConnection

from nemory.build_sources.internal.build_service import BuildService
from nemory.embeddings.provider import EmbeddingProvider
from nemory.services.chunk_embedding_service import ChunkEmbeddingService
from nemory.services.embedding_shard_resolver import EmbeddingShardResolver
from nemory.services.persistence_service import PersistenceService
from nemory.services.table_name_policy import TableNamePolicy
from nemory.storage.repositories.chunk_repository import ChunkRepository
from nemory.storage.repositories.datasource_run_repository import DatasourceRunRepository
from nemory.storage.repositories.embedding_model_registry_repository import EmbeddingModelRegistryRepository
from nemory.storage.repositories.embedding_repository import EmbeddingRepository
from nemory.storage.repositories.run_repository import RunRepository


def create_run_repository(conn: DuckDBPyConnection) -> RunRepository:
    return RunRepository(conn)


def create_datasource_run_repository(conn: DuckDBPyConnection) -> DatasourceRunRepository:
    return DatasourceRunRepository(conn)


def create_chunk_repository(conn: DuckDBPyConnection) -> ChunkRepository:
    return ChunkRepository(conn)


def create_embedding_repository(conn: DuckDBPyConnection) -> EmbeddingRepository:
    return EmbeddingRepository(conn)


def create_registry_repository(conn: DuckDBPyConnection) -> EmbeddingModelRegistryRepository:
    return EmbeddingModelRegistryRepository(conn)


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
    provider: EmbeddingProvider,
) -> ChunkEmbeddingService:
    resolver = create_shard_resolver(conn)
    persistence = create_persistence_service(conn)
    return ChunkEmbeddingService(persistence_service=persistence, provider=provider, shard_resolver=resolver)

def create_build_service(
        conn: DuckDBPyConnection,
        *,
        provider: EmbeddingProvider,
) -> BuildService:
    run_repo = create_run_repository(conn)
    datasource_run_repo = create_datasource_run_repository(conn)
    chunk_embedding_service = create_chunk_embedding_service(conn, provider=provider)

    return BuildService(
        run_repo=run_repo,
        datasource_run_repo=datasource_run_repo,
        chunk_embedding_service=chunk_embedding_service,
    )