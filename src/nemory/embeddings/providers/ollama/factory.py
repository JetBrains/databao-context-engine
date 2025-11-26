from .config import OllamaConfig
from .install import resolve_ollama_bin
from .runtime import OllamaRuntime
from .service import OllamaService
from .provider import OllamaEmbeddingProvider


def create_ollama_provider(
    *,
    host: str = "127.0.0.1",
    port: int = 11434,
    model_id: str = "nomic-embed-text:latest",
    dim: int = 768,
    ensure_ready: bool = True,
) -> OllamaEmbeddingProvider:
    bin_path = resolve_ollama_bin()

    config = OllamaConfig(host=host, port=port, bin_path=bin_path)
    service = OllamaService(config)

    if ensure_ready:
        runtime = OllamaRuntime(config=config, service=service)
        runtime.start_and_await(timeout=120)
        service.pull_model(model=model_id, timeout=900)

    return OllamaEmbeddingProvider(service=service, model_id=model_id, dim=dim)
