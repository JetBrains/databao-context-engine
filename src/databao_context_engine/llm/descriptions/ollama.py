from typing_extensions import override

from databao_context_engine.llm.descriptions.provider import DescriptionProvider
from databao_context_engine.llm.service import OllamaService


class OllamaDescriptionProvider(DescriptionProvider):
    def __init__(self, *, service: OllamaService, model_id: str):
        self._service = service
        self._model_id = model_id

    @property
    def describer(self) -> str:
        return "ollama"

    @property
    def model_id(self) -> str:
        return self._model_id

    @override
    def describe(self, text: str, context: str) -> str:
        description_prompt = self.default_description_prompt(text=text, context=context)

        return self._service.prompt(model=self._model_id, prompt=description_prompt)

    @override
    def prompt_for_description(self, prompt: str) -> str:
        return self._service.prompt(model=self._model_id, prompt=prompt)
