import duckdb

from databao_context_engine.services.models import ChunkEmbedding
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
        dim: int | None = None,
    ):
        self._conn = conn
        self._chunk_repo = chunk_repo
        self._embedding_repo = embedding_repo
        self._dim = dim or 768

    def write_chunks_and_embeddings(
        self,
        *,
        chunk_embeddings: list[ChunkEmbedding],
        table_name: str,
        full_type: str,
        datasource_id: str,
        override: bool = False,
    ):
        """Atomically persist chunks and their vectors.

        If override is True, delete existing chunks and embeddings for the datasource before persisting.

        Raises:
            ValueError: If chunk_embeddings is an empty list.

        """
        if not chunk_embeddings:
            raise ValueError("chunk_embeddings must be a non-empty list")

        # Outside the transaction due to duckdb limitations.
        # DuckDB FK checks can behave unexpectedly across multiple statements in the same transaction when deleting
        # and re-inserting related rows. It also does not support on delete cascade yet.
        # Given that there is a foreign key from embedding to chunk, the embedding must be deleted first.
        if override:
            self._embedding_repo.delete_by_datasource_id(table_name=table_name, datasource_id=datasource_id)
            self._chunk_repo.delete_by_datasource_id(datasource_id=datasource_id)

        with transaction(self._conn):
            chunk_ids = self._chunk_repo.bulk_insert(
                full_type=full_type,
                datasource_id=datasource_id,
                chunk_contents=[(ce.chunk.embeddable_text, ce.display_text) for ce in chunk_embeddings],
            )

            self._embedding_repo.bulk_insert(
                table_name=table_name,
                chunk_ids=chunk_ids,
                vecs=[ce.vec for ce in chunk_embeddings],
                dim=self._dim,
            )
