from nemory.llm.descriptions.provider import DescriptionProvider
from nemory.llm.service import OllamaService


class OllamaDescriptionProvider(DescriptionProvider):
    def __init__(self, *, service: OllamaService, model_id: str):
        self._service = service
        self._model_id = model_id

    @property
    def description(self) -> str:
        return "ollama"

    @property
    def model_id(self) -> str:
        return self._model_id

    def describe(self, text: str, context: str) -> str:
        description = self._service.describe(model=self._model_id, text=text, context=context)

        return description
