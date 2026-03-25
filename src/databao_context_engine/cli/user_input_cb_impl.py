from typing import Any

import click

from databao_context_engine import UserInputCallback
from databao_context_engine.datasources.config_wizard import Choice


class ClickUserInputCallback(UserInputCallback):
    def prompt(
        self,
        property_key: str,
        required: bool,
        type: Choice | Any | None = None,
        default_value: Any | None = None,
        is_secret: bool = False,
    ) -> Any:
        show_default: bool = default_value is not None and default_value != ""
        final_type = click.Choice(type.choices) if isinstance(type, Choice) else str

        # click goes infinite loop if user gives emptry string as an input AND default_value is None
        # in order to exit this loop we need to set default value to '' (so it gets accepted)
        #
        # Code snippet from click:
        # while True:
        #   value = prompt_func(prompt)
        #     if value:
        #       break
        #     elif default is not None:
        #       value = default
        #       break
        default_value = str(default_value) if default_value else "" if final_type is str else None
        prompt_text = f"{property_key}?{' (Optional)' if not required else ''}"
        return click.prompt(
            text=prompt_text, default=default_value, hide_input=is_secret, type=final_type, show_default=show_default
        )

    def confirm(self, text: str) -> bool:
        return click.confirm(text=text)

    def on_validation_error(self, property_key: str, input_value: Any, error_message: str) -> None:
        click.echo(error_message)
