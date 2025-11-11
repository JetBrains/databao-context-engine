from collections.abc import Sequence

import requests

from nemory.core.services.providers.base import (
    EmbeddingProvider,
    EmbeddingProviderTransientError,
    EmbeddingProviderPermanentError,
)
from nemory.core.services.providers.ollama.service import OllamaService


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

    def embed(self, text: str) -> Sequence[float]:
        try:
            vec = self._service.embed(model=self._model_id, text=text)
        except TimeoutError as e:
            raise EmbeddingProviderTransientError(str(e)) from e
        except requests.HTTPError as e:
            raise EmbeddingProviderPermanentError(str(e)) from e
        except requests.RequestException as e:
            raise EmbeddingProviderTransientError(str(e)) from e
        except Exception as e:
            raise EmbeddingProviderPermanentError(str(e)) from e

        if len(vec) != self._dim:
            raise EmbeddingProviderPermanentError(f"provider returned dim={len(vec)} but expected {self._dim}")

        return [float(x) for x in vec]
