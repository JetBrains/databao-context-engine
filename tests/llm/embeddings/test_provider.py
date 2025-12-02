from unittest.mock import Mock

import pytest

from nemory.llm.embeddings.ollama import OllamaEmbeddingProvider
from nemory.llm.service import OllamaService


def test_embed_calls_service_and_returns_vec():
    service = Mock(spec=OllamaService)
    service.embed.return_value = [1, 2.5]

    provider = OllamaEmbeddingProvider(service=service, model_id="nomic-embed-text", dim=2)

    vec = provider.embed("hello world")

    service.embed.assert_called_once_with(model="nomic-embed-text", text="hello world")
    assert vec == [1.0, 2.5]
    assert provider.model_id == "nomic-embed-text"
    assert provider.embedder == "ollama"
    assert provider.dim == 2


def test_embed_raises_on_wrong_dim():
    service = Mock(spec=OllamaService)
    service.embed.return_value = [1.0]

    provider = OllamaEmbeddingProvider(service=service, model_id="m", dim=2)

    with pytest.raises(ValueError, match="provider returned dim=1 but expected 2"):
        provider.embed("x")
