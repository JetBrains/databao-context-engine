from duckdb import DuckDBPyConnection

from databao_context_engine.storage.repositories.chunk_repository import ChunkRepository
from databao_context_engine.storage.repositories.chunk_search_repository import ChunkSearchRepository
from databao_context_engine.storage.repositories.embedding_model_registry_repository import (
    EmbeddingModelRegistryRepository,
)
from databao_context_engine.storage.repositories.embedding_repository import EmbeddingRepository


def create_chunk_repository(conn: DuckDBPyConnection) -> ChunkRepository:
    return ChunkRepository(conn)


def create_embedding_repository(conn: DuckDBPyConnection) -> EmbeddingRepository:
    return EmbeddingRepository(conn)


def create_registry_repository(conn: DuckDBPyConnection) -> EmbeddingModelRegistryRepository:
    return EmbeddingModelRegistryRepository(conn)


def create_chunk_search_repository(conn: DuckDBPyConnection) -> ChunkSearchRepository:
    return ChunkSearchRepository(conn)
