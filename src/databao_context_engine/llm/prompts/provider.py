from typing import Protocol


class PromptProvider(Protocol):
    @property
    def prompter(self) -> str: ...
    @property
    def model_id(self) -> str: ...

    def prompt(self, prompt: str) -> str: ...
