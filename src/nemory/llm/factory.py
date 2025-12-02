from nemory.llm.config import OllamaConfig
from nemory.llm.descriptions.ollama import OllamaDescriptionProvider
from nemory.llm.install import resolve_ollama_bin
from nemory.llm.embeddings.ollama import OllamaEmbeddingProvider
from nemory.llm.runtime import OllamaRuntime
from nemory.llm.service import OllamaService


def _create_ollama_service_common(
    *,
    host: str,
    port: int,
    ensure_ready: bool,
) -> OllamaService:
    bin_path = resolve_ollama_bin()
    config = OllamaConfig(host=host, port=port, bin_path=bin_path)
    service = OllamaService(config)

    if ensure_ready:
        runtime = OllamaRuntime(config=config, service=service)
        runtime.start_and_await(timeout=120)

    return service


def create_ollama_service(
    *,
    host: str = "127.0.0.1",
    port: int = 11434,
    ensure_ready: bool = True,
) -> OllamaService:
    return _create_ollama_service_common(
        host=host,
        port=port,
        ensure_ready=ensure_ready,
    )


def create_ollama_embedding_provider(
    service: OllamaService,
    *,
    model_id: str = "nomic-embed-text:v1.5",
    dim: int = 768,
    pull_if_needed: bool = True,
) -> OllamaEmbeddingProvider:
    if pull_if_needed:
        service.pull_model_if_needed(model=model_id, timeout=900)

    return OllamaEmbeddingProvider(service=service, model_id=model_id, dim=dim)


def create_ollama_description_provider(
    service: OllamaService,
    *,
    model_id: str = "llama3.2:1b",
    pull_if_needed: bool = True,
):
    if pull_if_needed:
        service.pull_model_if_needed(model=model_id, timeout=900)

    return OllamaDescriptionProvider(service=service, model_id=model_id)
