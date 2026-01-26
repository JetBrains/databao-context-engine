from _duckdb import DuckDBPyConnection

from databao_context_engine.storage.repositories.chunk_repository import ChunkRepository
from databao_context_engine.storage.repositories.embedding_model_registry_repository import (
    EmbeddingModelRegistryRepository,
)
from databao_context_engine.storage.repositories.embedding_repository import EmbeddingRepository
from databao_context_engine.storage.repositories.vector_search_repository import VectorSearchRepository


def create_chunk_repository(conn: DuckDBPyConnection) -> ChunkRepository:
    return ChunkRepository(conn)


def create_embedding_repository(conn: DuckDBPyConnection) -> EmbeddingRepository:
    return EmbeddingRepository(conn)


def create_registry_repository(conn: DuckDBPyConnection) -> EmbeddingModelRegistryRepository:
    return EmbeddingModelRegistryRepository(conn)


def create_vector_search_repository(conn: DuckDBPyConnection) -> VectorSearchRepository:
    return VectorSearchRepository(conn)
