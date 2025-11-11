import duckdb

from nemory.core.db.embedding_repository import EmbeddingRepository
from nemory.core.db.segment_repository import SegmentRepository
from nemory.core.db.tx import transaction
from nemory.core.services.models import EmbeddingItem
from nemory.pluginlib.build_plugin import EmbeddableChunk


class PersistenceService:
    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        segment_repo: SegmentRepository,
        embedding_repo: EmbeddingRepository,
        *,
        dim: int = 768,
    ):
        self._conn = conn
        self._segment_repo = segment_repo
        self._embedding_repo = embedding_repo
        self._dim = dim

    def write_segments(self, *, entity_id: int, chunks: list[EmbeddableChunk]) -> list[int]:
        """
        Persists all segments for an already persisted entity

        - Writes the segments atomically
        - On any error, the transaction is rolled back, and no segments are created.
        """
        if chunks is None:
            raise ValueError("chunks must not be None")

        created_segment_ids: list[int] = []

        with transaction(self._conn):
            for chunk in chunks:
                segment = self._segment_repo.create(
                    entity_id=entity_id, embeddable_text=chunk.embeddable_text, display_text=chunk.content
                )
                created_segment_ids.append(segment.segment_id)
        return created_segment_ids

    def write_embeddings(self, *, items: list[EmbeddingItem], embedder: str, model_id: str) -> int:
        """
        Persist embeddings for segments

        - Validates vector length before opening the transaction
        - Writes the batch atomically
        - On any error, the transaction is rolled back, and no embeddings are created.
        """
        if not items:
            raise ValueError("Items must be a non-empty list of EmbeddingItem")

        for item in items:
            if len(item.vec) != self._dim:
                raise ValueError(
                    f"embedding vec must be length {self._dim}, got {len(item.vec)} (segment_id={item.segment_id})"
                )

        inserted = 0
        with transaction(self._conn):
            for item in items:
                self._embedding_repo.create(
                    segment_id=item.segment_id, embedder=embedder, model_id=model_id, vec=item.vec
                )
                inserted += 1
        return inserted
