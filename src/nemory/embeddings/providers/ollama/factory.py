from .config import OllamaConfig
from .service import OllamaService
from .provider import OllamaEmbeddingProvider


def create_ollama_provider(
    *,
    host: str = "127.0.0.1",
    port: int = 11434,
    model_id: str = "nomic-embed-text:latest",
    dim: int = 768,
) -> OllamaEmbeddingProvider:
    cfg = OllamaConfig(host=host, port=port)
    svc = OllamaService(cfg)
    return OllamaEmbeddingProvider(service=svc, model_id=model_id, dim=dim)
