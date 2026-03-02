from collections.abc import Sequence

from databao_context_engine.llm.config import EmbeddingModelDetails
from databao_context_engine.llm.embeddings.provider import EmbeddingProvider
from databao_context_engine.llm.service import OllamaService


class OllamaEmbeddingProvider(EmbeddingProvider):
    def __init__(self, *, service: OllamaService, model_details: EmbeddingModelDetails, embed_batch_size: int = 128):
        self._service = service
        self._model_details = model_details
        self._embed_batch_size: int = embed_batch_size

    @property
    def embedder(self) -> str:
        return "ollama"

    @property
    def embedding_model_details(self) -> EmbeddingModelDetails:
        return self._model_details

    def embed(self, text: str) -> Sequence[float]:
        vec = self._service.embed(model=self._model_details.model_id, text=text)

        if len(vec) != self._model_details.model_dim:
            raise ValueError(f"provider returned dim={len(vec)} but expected {self._model_details.model_dim}")

        return [float(x) for x in vec]

    def embed_many(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        vecs: list[list[float]] = []
        for i in range(0, len(texts), self._embed_batch_size):
            batch = texts[i : i + self._embed_batch_size]
            vecs.extend(self._service.embed_many(model=self._model_details.model_id, texts=batch))

        if len(vecs) != len(texts):
            raise ValueError(f"provider returned {len(vecs)} vectors for {len(texts)} texts")

        return vecs
