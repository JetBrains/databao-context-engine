from collections.abc import Sequence

import duckdb

from databao_context_engine.progress.progress import EMIT_EVERY, ProgressCallback, ProgressEmitter
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
        self,
        *,
        chunk_embeddings: list[ChunkEmbedding],
        table_name: str,
        full_type: str,
        datasource_id: str,
        progress: ProgressCallback | None = None,
    ):
        """Atomically persist chunks and their vectors.

        Raises:
            ValueError: If chunk_embeddings is an empty list.

        """
        if not chunk_embeddings:
            raise ValueError("chunk_embeddings must be a non-empty list")

        emitter = ProgressEmitter(progress)
        total_items = len(chunk_embeddings)

        with transaction(self._conn):
            for i, chunk_embedding in enumerate(chunk_embeddings, start=1):
                chunk_dto = self.create_chunk(
                    full_type=full_type,
                    datasource_id=datasource_id,
                    embeddable_text=chunk_embedding.chunk.embeddable_text,
                    display_text=chunk_embedding.display_text,
                )
                self.create_embedding(table_name=table_name, chunk_id=chunk_dto.chunk_id, vec=chunk_embedding.vec)

                if i % EMIT_EVERY == 0 or i == total_items:
                    total_units = total_items * 2
                    emitter.datasource_progress_units(
                        datasource_id=datasource_id,
                        completed_units=total_items + i,
                        total_units=total_units,
                    )
        emitter.datasource_progress_units(
            datasource_id=datasource_id,
            completed_units=total_items * 2,
            total_units=total_items * 2,
        )

    def create_chunk(self, *, full_type: str, datasource_id: str, embeddable_text: str, display_text: str) -> ChunkDTO:
        return self._chunk_repo.create(
            full_type=full_type,
            datasource_id=datasource_id,
            embeddable_text=embeddable_text,
            display_text=display_text,
        )

    def create_embedding(self, *, table_name: str, chunk_id: int, vec: Sequence[float]):
        self._embedding_repo.create(
            table_name=table_name,
            chunk_id=chunk_id,
            vec=vec,
        )
