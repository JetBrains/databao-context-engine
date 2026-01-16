from collections.abc import Sequence

import duckdb

from databao_context_engine.services.models import ChunkEmbedding
from databao_context_engine.storage.models import ChunkDTO
from databao_context_engine.storage.repositories.chunk_repository import ChunkRepository
from databao_context_engine.storage.repositories.embedding_repository import EmbeddingRepository
from databao_context_engine.storage.transaction import transaction


class PersistenceService:
    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        chunk_repo: ChunkRepository,
        embedding_repo: EmbeddingRepository,
        *,
        dim: int = 768,
    ):
        self._conn = conn
        self._chunk_repo = chunk_repo
        self._embedding_repo = embedding_repo
        self._dim = dim

    def write_chunks_and_embeddings(
        self, *, datasource_run_id: int, chunk_embeddings: list[ChunkEmbedding], table_name: str
    ):
        """
        Atomically persist chunks and their vectors.
        Returns the number of embeddings written.
        """
        if not chunk_embeddings:
            raise ValueError("chunk_embeddings must be a non-empty list")

        with transaction(self._conn):
            for chunk_embedding in chunk_embeddings:
                chunk_dto = self.create_chunk(
                    datasource_run_id=datasource_run_id,
                    embeddable_text=chunk_embedding.chunk.embeddable_text,
                    display_text=chunk_embedding.display_text,
                    generated_description=chunk_embedding.generated_description,
                )
                self.create_embedding(table_name=table_name, chunk_id=chunk_dto.chunk_id, vec=chunk_embedding.vec)

    def create_chunk(
        self, *, datasource_run_id: int, embeddable_text: str, display_text: str, generated_description: str
    ) -> ChunkDTO:
        return self._chunk_repo.create(
            datasource_run_id=datasource_run_id,
            embeddable_text=embeddable_text,
            display_text=display_text,
            generated_description=generated_description,
        )

    def create_embedding(self, *, table_name: str, chunk_id: int, vec: Sequence[float]):
        self._embedding_repo.create(
            table_name=table_name,
            chunk_id=chunk_id,
            vec=vec,
        )
