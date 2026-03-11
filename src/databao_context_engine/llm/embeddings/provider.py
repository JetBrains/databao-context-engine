from collections.abc import Sequence
from typing import Protocol

from databao_context_engine.llm.config import EmbeddingModelDetails


class EmbeddingProvider(Protocol):
    @property
    def embedder(self) -> str: ...
    @property
    def embedding_model_details(self) -> EmbeddingModelDetails: ...

    def embed(self, text: str) -> Sequence[float]: ...

    def embed_many(self, texts: list[str]) -> list[list[float]]: ...
