from typing import Any, Iterable, Protocol

from databao_context_engine.pluginlib.config import ConfigPropertyDefinition, ConfigUnionPropertyDefinition


class UserInputCallback(Protocol):
    """Callback which is called in an interactive session when some input from the user is needed."""

    def prompt(
        self,
        text: str,
        type: Iterable[str] | Any | None = None,
        default: Any | None = None,
        hide_input: bool = False,
        show_default: bool = True,
    ) -> str: ...

    def confirm(self, text: str) -> bool: ...


def build_config_content_interactively(
    properties: list[ConfigPropertyDefinition], user_input_callback: UserInputCallback
) -> dict[str, Any]:
    return _build_config_content_from_properties(properties=properties, user_input_callback=user_input_callback)


def _build_config_content_from_properties(
    properties: list[ConfigPropertyDefinition],
    user_input_callback: UserInputCallback,
    properties_prefix: str = "",
    in_union: bool = False,
) -> dict[str, Any]:
    config_content: dict[str, Any] = {}
    for config_file_property in properties:
        if config_file_property.property_key in ["type", "name"] and len(properties_prefix) == 0:
            # We ignore type and name properties as they've already been filled
            continue
        if in_union and config_file_property.property_key == "type":
            continue

        if isinstance(config_file_property, ConfigUnionPropertyDefinition):
            choices = {t.__name__: t for t in config_file_property.types}

            chosen = user_input_callback.prompt(
                text=f"{properties_prefix}{config_file_property.property_key}.type?",
                type=sorted(choices.keys()),
            )

            chosen_type = choices[chosen]

            nested_props = config_file_property.type_properties[chosen_type]
            nested_content = _build_config_content_from_properties(
                nested_props,
                user_input_callback=user_input_callback,
                properties_prefix=f"{properties_prefix}{config_file_property.property_key}.",
                in_union=True,
            )

            config_content[config_file_property.property_key] = {
                **nested_content,
            }
            continue

        if config_file_property.nested_properties is not None and len(config_file_property.nested_properties) > 0:
            fq_property_name = (
                f"{properties_prefix}.{config_file_property.property_key}"
                if properties_prefix
                else f"{config_file_property.property_key}"
            )
            if not config_file_property.required:
                if not user_input_callback.confirm(f"\nAdd {fq_property_name}?"):
                    continue

            nested_content = _build_config_content_from_properties(
                config_file_property.nested_properties,
                user_input_callback=user_input_callback,
                properties_prefix=f"{fq_property_name}.",
            )
            if len(nested_content.keys()) > 0:
                config_content[config_file_property.property_key] = nested_content
        else:
            default_value: str | None
            if config_file_property.default_value:
                default_value = config_file_property.default_value
            else:
                # We need to add an empty string default value for non-required fields
                default_value = None if config_file_property.required else ""

            property_value = user_input_callback.prompt(
                text=f"{properties_prefix}{config_file_property.property_key}? {'(Optional)' if not config_file_property.required else ''}",
                type=str,
                default=default_value,
                show_default=default_value is not None and default_value != "",
                hide_input=config_file_property.secret,
            )

            if property_value.strip():
                config_content[config_file_property.property_key] = property_value

    return config_content
