from dataclasses import dataclass

from databao_context_engine.llm.config import EmbeddingModelDetails


@dataclass(frozen=True)
class FakeOllamaEmbeddingProvider:
    embedder: str = "fake"
    embedding_model_details: EmbeddingModelDetails = EmbeddingModelDetails(model_id="dummy", model_dim=768)

    def embed(self, text: str) -> list[float]:
        seed = float(len(text) % 10)
        return [seed] * self.embedding_model_details.model_dim

    def embed_many(self, texts: list[str]) -> list[list[float]]:
        return [self.embed(text) for text in texts]
