from databao_context_engine.llm.prompts.provider import PromptProvider
from databao_context_engine.llm.service import OllamaService


class OllamaPromptProvider(PromptProvider):
    def __init__(self, *, service: OllamaService, model_id: str):
        self._service = service
        self._model_id = model_id

    @property
    def prompter(self) -> str:
        return "ollama"

    @property
    def model_id(self) -> str:
        return self._model_id

    def prompt(self, prompt: str) -> str:
        return self._service.prompt(model=self.model_id, prompt=prompt)
