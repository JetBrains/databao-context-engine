from collections.abc import Sequence

from nemory.llm.embeddings.provider import EmbeddingProvider
from nemory.llm.service import OllamaService


class OllamaEmbeddingProvider(EmbeddingProvider):
    def __init__(
        self,
        *,
        service: OllamaService,
        model_id: str,
        dim: int = 768,
    ):
        self._service = service
        self._model_id = model_id
        self._dim: int = dim

    @property
    def embedder(self) -> str:
        return "ollama"

    @property
    def model_id(self) -> str:
        return self._model_id

    @property
    def dim(self) -> int:
        return self._dim

    def embed(self, text: str) -> Sequence[float]:
        vec = self._service.embed(model=self._model_id, text=text)

        if len(vec) != self._dim:
            raise ValueError(f"provider returned dim={len(vec)} but expected {self._dim}")

        return [float(x) for x in vec]
