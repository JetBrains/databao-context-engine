from typing import Protocol


class DescriptionProvider(Protocol):
    @property
    def describer(self) -> str: ...
    @property
    def model_id(self) -> str: ...

    def describe(self, text: str, context: str) -> str: ...
