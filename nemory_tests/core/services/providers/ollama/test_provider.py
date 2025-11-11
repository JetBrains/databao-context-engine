import pytest
import requests

from nemory.core.services.providers.ollama.provider import OllamaEmbeddingProvider
from nemory.core.services.providers.base import (
    EmbeddingProviderTransientError,
    EmbeddingProviderPermanentError,
)


class _StubSvc:
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
    svc = _StubSvc()
    svc.set_vec([1.0, 2.0])
    prov = OllamaEmbeddingProvider(service=svc, model_id="nomic-embed-text", dim=2)

    v = prov.embed("hello")
    assert v == [1.0, 2.0]
    assert prov.model_id == "nomic-embed-text"


def test_wrong_dim_raises_permanent():
    svc = _StubSvc()
    svc.set_vec([1.0])
    prov = OllamaEmbeddingProvider(service=svc, model_id="m", dim=2)

    with pytest.raises(EmbeddingProviderPermanentError):
        prov.embed("x")


def test_timeouts_raise_transient():
    svc = _StubSvc()
    svc.set_exc(TimeoutError("boom"))
    prov = OllamaEmbeddingProvider(service=svc, model_id="m", dim=2)

    with pytest.raises(EmbeddingProviderTransientError):
        prov.embed("x")


def test_http_error_raises_permanent():
    svc = _StubSvc()
    svc.set_exc(requests.HTTPError("500"))
    prov = OllamaEmbeddingProvider(service=svc, model_id="m", dim=2)

    with pytest.raises(EmbeddingProviderPermanentError):
        prov.embed("x")


def test_transport_error_raises_transient():
    svc = _StubSvc()
    svc.set_exc(requests.RequestException("net down"))
    prov = OllamaEmbeddingProvider(service=svc, model_id="m", dim=2)

    with pytest.raises(EmbeddingProviderTransientError):
        prov.embed("x")
