from typing import Any

from databao_context_engine.datasources.config_wizard import Choice, UserInputCallback


class MockUserInputCallback(UserInputCallback):
    def __init__(self, inputs: list[Any] | None = None):
        self.inputs = inputs or []
        self.input_index = 0

    def prompt(
        self,
        text: str,
        type: Choice | Any | None = None,
        default_value: Any | None = None,
        is_secret: bool = False,
    ) -> Any:
        if self.input_index >= len(self.inputs):
            raise AssertionError("Not enough inputs")

        val = self.inputs[self.input_index]
        self.input_index += 1

        if val == "" and default_value is not None:
            return default_value

        return val

    def confirm(self, text: str) -> bool:
        if self.input_index >= len(self.inputs):
            raise AssertionError("Not enough inputs")
        val = self.inputs[self.input_index]
        self.input_index += 1
        if isinstance(val, bool):
            return val
        raise AssertionError(f"Expected boolean val but {type(val)}:{repr(val)} is provided")
