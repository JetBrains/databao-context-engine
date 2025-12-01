from .config import OllamaConfig
from .install import resolve_ollama_bin
from .provider import OllamaEmbeddingProvider
from .runtime import OllamaRuntime
from .service import OllamaService


def _create_ollama_service_common(
    *,
    host: str,
    port: int,
    model_id: str,
    ensure_ready: bool,
) -> OllamaService:
    bin_path = resolve_ollama_bin()
    config = OllamaConfig(host=host, port=port, bin_path=bin_path)
    service = OllamaService(config)

    if ensure_ready:
        runtime = OllamaRuntime(config=config, service=service)
        runtime.start_and_await(timeout=120)
        service.pull_model_if_needed(model=model_id, timeout=900)

    return service


def create_ollama_provider(
    *,
    host: str = "127.0.0.1",
    port: int = 11434,
    model_id: str = "nomic-embed-text:v1.5",
    dim: int = 768,
    ensure_ready: bool = True,
) -> OllamaEmbeddingProvider:
    service = _create_ollama_service_common(
        host=host,
        port=port,
        model_id=model_id,
        ensure_ready=ensure_ready,
    )
    return OllamaEmbeddingProvider(service=service, model_id=model_id, dim=dim)


def create_ollama_service(
    *,
    host: str = "127.0.0.1",
    port: int = 11434,
    model_id: str = "llama3.2:1b",
    ensure_ready: bool = True,
) -> OllamaService:
    return _create_ollama_service_common(
        host=host,
        port=port,
        model_id=model_id,
        ensure_ready=ensure_ready,
    )
