from _duckdb import DuckDBPyConnection

from nemory.services.run_name_policy import RunNamePolicy
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
