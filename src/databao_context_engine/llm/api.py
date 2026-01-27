from pathlib import Path

from databao_context_engine.llm.errors import OllamaError
from databao_context_engine.llm.factory import (
    DEFAULT_DESCRIPTION_GENERATOR_MODEL,
    DEFAULT_EMBED_MODEL_ID,
    create_ollama_service,
)
from databao_context_engine.llm.install import resolve_ollama_bin


def install_ollama_if_needed() -> Path:
    """Install the Ollama CLI locally if needed.

    This will look for any existing installation of Ollama on the system. If none is found, it will install it locally.

    Here is the priority order of how it looks for an installed Ollama CLI binary:
    1. Look at the path defined in the DCE_OLLAMA_BIN env var, if it is set
    2. Look for `ollama` in the PATH
    3. Look for a DCE-managed installation in the global DCE path

    If Ollama is not found, it will get installed as a DCE-managed installation in the global DCE path.

    Returns:
        The path to the Ollama CLI executable.
    """
    return Path(resolve_ollama_bin())


def download_ollama_models_if_needed(
    *, download_embed_model: bool = True, download_description_generator_model: bool = False
) -> None:
    """Download the Ollama models required to run DCE if needed.

    If the models were already downloaded, this method will do nothing.

    If no Ollama CLI is found on the system, this method will install one as a DCE-managed installation in the global DCE path.

    Args:
        download_embed_model: Whether to download the embedding model.
        download_description_generator_model: Whether to download the description generator model.

    Raises:
        OllamaError: If there is an error downloading one of the models.
    """
    ollama_service = create_ollama_service()

    if download_embed_model:
        try:
            ollama_service.pull_model_if_needed(model=DEFAULT_EMBED_MODEL_ID)
        except OllamaError as e:
            raise e
    if download_description_generator_model:
        try:
            ollama_service.pull_model_if_needed(model=DEFAULT_DESCRIPTION_GENERATOR_MODEL)
        except OllamaError as e:
            raise e
