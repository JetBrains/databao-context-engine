import logging
from dataclasses import dataclass
from typing import Any, Iterable, Protocol

from pydantic import TypeAdapter, ValidationError

from databao_context_engine.pluginlib.config import (
    ConfigPropertyDefinition,
    ConfigUnionPropertyDefinition,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Choice:
    choices: Iterable[str]


class UserInputCallback(Protocol):
    """Callback which is called in an interactive session when some input from the user is needed."""

    def prompt(
        self,
        property_key: str,
        required: bool,
        type: Choice | Any | None = None,
        default_value: Any | None = None,
        is_secret: bool = False,
    ) -> Any: ...

    def confirm(self, text: str) -> bool: ...

    def on_validation_error(self, property_key: str, input_value: Any, error_message: str) -> None:
        """Called when a validation error occurs on a value inputed by the user.

        It gives a chance to show that error to user in the UI if necessary.
        """
        pass


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
        if config_file_property.property_key in ["type", "name", "enabled"] and len(properties_prefix) == 0:
            # We ignore type and name properties as they've already been filled
            continue
        if in_union and config_file_property.property_key == "type":
            continue

        if isinstance(config_file_property, ConfigUnionPropertyDefinition):
            choices = {t.__name__: t for t in config_file_property.types}
            default_choice = config_file_property.default_type.__name__ if config_file_property.default_type else None

            chosen = user_input_callback.prompt(
                property_key=f"{properties_prefix}{config_file_property.property_key}.type",
                required=True,
                type=Choice(sorted(choices.keys())),
                default_value=default_choice,
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
            received_valid_user_input = False
            while not received_valid_user_input:
                full_property_key = f"{properties_prefix}{config_file_property.property_key}"
                property_value = user_input_callback.prompt(
                    property_key=full_property_key,
                    required=config_file_property.required,
                    type=config_file_property.property_type,
                    default_value=config_file_property.default_value,
                    is_secret=config_file_property.secret,
                )

                normalized_property_value = _normalize_value(property_value)

                if normalized_property_value is None:
                    if config_file_property.required:
                        # No value provided even though it is mandatory, ask for prompt again
                        user_input_callback.on_validation_error(
                            property_key=full_property_key,
                            input_value=property_value,
                            error_message="Field is required",
                        )
                        continue
                    # Empty value is valid for non-required fields
                    received_valid_user_input = True
                else:
                    try:
                        validated_property = _validate_property_value(
                            config_file_property.property_type, normalized_property_value
                        )

                        config_content[config_file_property.property_key] = validated_property
                        received_valid_user_input = True
                    except ValidationError as e:
                        # Validation error for the current input, ask for prompt again
                        user_input_callback.on_validation_error(
                            property_key=full_property_key,
                            input_value=property_value,
                            error_message=e.errors()[0].get("msg") if e.errors() else "",
                        )
                        continue

    return config_content


def _normalize_value(property_value) -> str | None:
    if isinstance(property_value, str):
        stripped_str = property_value.strip()

        if len(stripped_str) == 0:
            # Ignore empty strings
            return None

        return stripped_str

    return property_value


def _validate_property_value(property_type: type | None, property_value: Any) -> Any:
    if property_type is None:
        return property_value

    try:
        return TypeAdapter(property_type).validate_python(property_value)
    except ValidationError as e:
        raise e
    except Exception:
        # Handling any error related to the type not being usable as a TypeAdapter:
        #  In that case, we don't validate the value and simply return it as-is
        logger.debug("Failed to validate property", exc_info=True)
        return property_value
