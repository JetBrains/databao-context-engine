import pytest
import requests

from nemory.embeddings.providers.ollama.provider import OllamaEmbeddingProvider
from nemory.embeddings.provider import (
    EmbeddingProviderTransientError,
    EmbeddingProviderPermanentError,
)


class _StubService:
    def __init__(self):
        self.calls = []
        self.next = [0.0, 1.0]

    def set_vec(self, v):
        self.next = v

    def set_exc(self, e):
        self.next = e

    def embed(self, *, model: str, text: str):
        self.calls.append((model, text))
        if isinstance(self.next, Exception):
            raise self.next
        return list(self.next)


def test_embed():
    service = _StubService()
    service.set_vec([1.0, 2.0])
    prov = OllamaEmbeddingProvider(service=service, model_id="nomic-embed-text", dim=2)

    v = prov.embed("hello")
    assert v == [1.0, 2.0]
    assert prov.model_id == "nomic-embed-text"


def test_wrong_dim_raises_permanent():
    service = _StubService()
    service.set_vec([1.0])
    prov = OllamaEmbeddingProvider(service=service, model_id="m", dim=2)

    with pytest.raises(EmbeddingProviderPermanentError):
        prov.embed("x")


def test_timeouts_raise_transient():
    service = _StubService()
    service.set_exc(TimeoutError("boom"))
    prov = OllamaEmbeddingProvider(service=service, model_id="m", dim=2)

    with pytest.raises(EmbeddingProviderTransientError):
        prov.embed("x")


def test_http_error_raises_permanent():
    service = _StubService()
    service.set_exc(requests.HTTPError("500"))
    prov = OllamaEmbeddingProvider(service=service, model_id="m", dim=2)

    with pytest.raises(EmbeddingProviderPermanentError):
        prov.embed("x")


def test_transport_error_raises_transient():
    service = _StubService()
    service.set_exc(requests.RequestException("net down"))
    prov = OllamaEmbeddingProvider(service=service, model_id="m", dim=2)

    with pytest.raises(EmbeddingProviderTransientError):
        prov.embed("x")
