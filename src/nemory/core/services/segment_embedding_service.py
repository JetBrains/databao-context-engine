from nemory.core.services.models import EmbeddingItem
from nemory.core.services.persistence_service import PersistenceService
from nemory.core.services.providers.base import EmbeddingProvider
from nemory.pluginlib.build_plugin import EmbeddableChunk


class SegmentEmbeddingService:
    def __init__(
        self,
        *,
        persistence_service: PersistenceService,
        provider: EmbeddingProvider,
    ):
        self._persistence_service = persistence_service
        self._provider = provider

    def embed_chunks(self, *, entity_id: int, chunks: list[EmbeddableChunk]) -> None:
        """
        Turn plugin chunks into persisted segments and embeddings

        Flow:
        1) Persist segments for the given chunks
        2) For each chunk, embed the chunk
        3) Persist embeddings vectors
        """

        if not chunks:
            return

        segment_ids = self._persistence_service.write_segments(entity_id=entity_id, chunks=chunks)
        if len(segment_ids) != len(chunks):
            raise RuntimeError(f"segment count mismatch (segments={len(segment_ids)} chunks={len(chunks)})")

        items: list[EmbeddingItem] = []
        for segment_id, chunk in zip(segment_ids, chunks):
            vec = self._provider.embed(chunk.embeddable_text)
            items.append(EmbeddingItem(segment_id=segment_id, vec=vec))

        self._persistence_service.write_embeddings(
            items=items, embedder=self._provider.embedder, model_id=self._provider.model_id
        )
