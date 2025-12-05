import types
from dataclasses import fields, is_dataclass, MISSING
from typing import Annotated, Union, get_origin, get_type_hints, Any

from nemory.pluginlib.config_properties import ConfigPropertyAnnotation, ConfigPropertyDefinition


def get_property_list_from_type(root_type: type) -> list[ConfigPropertyDefinition]:
    return _get_property_list_from_type(parent_type=root_type, is_root_type=True)


def _get_property_list_from_type(*, parent_type: type, is_root_type: bool) -> list[ConfigPropertyDefinition]:
    if is_dataclass(parent_type):
        return _get_property_list_from_dataclass(parent_type=parent_type)

    return _get_property_list_from_type_hints(parent_type=parent_type, is_root_type=is_root_type)


def _get_property_list_from_type_hints(*, parent_type: type, is_root_type: bool) -> list[ConfigPropertyDefinition]:
    try:
        type_hints = get_type_hints(parent_type, include_extras=True)
    except TypeError as e:
        if is_root_type:
            # Ignore root types that don't have type hints like dict or list
            return []
        else:
            # If we're evaluating a nested property, we want to propagate the exception
            # to let the parent property know that this type should be ignored
            raise e

    result = []
    for property_key, property_type in type_hints.items():
        config_property = _create_property(property_type=property_type, property_name=property_key)

        if config_property is not None:
            result.append(config_property)

    return result


def _get_property_list_from_dataclass(parent_type: type) -> list[ConfigPropertyDefinition]:
    if not is_dataclass(parent_type):
        raise ValueError(f"{parent_type} is not a dataclass")

    dataclass_fields = fields(parent_type)

    result = []
    for field in dataclass_fields:
        has_field_default = field.default is not None and field.default != MISSING

        property_for_field = _create_property(
            property_type=field.type,
            property_name=field.name,
            property_default=field.default if has_field_default else None,
            is_property_required=not has_field_default,
        )

        if property_for_field is not None:
            result.append(property_for_field)

    return result


def _create_property(
    *,
    property_type: type,
    property_name: str,
    property_default: Any | None = None,
    is_property_required: bool = False,
) -> ConfigPropertyDefinition | None:
    annotation = _get_config_property_annotation(property_type)

    if annotation is not None and annotation.ignored_for_config_wizard:
        return None

    actual_property_type = _read_actual_property_type(property_type)

    try:
        nested_properties = _get_property_list_from_type(parent_type=actual_property_type, is_root_type=False)
    except TypeError:
        return None

    default_value = compute_default_value(
        annotation=annotation,
        property_default=property_default,
        has_nested_properties=nested_properties is not None and len(nested_properties) > 0,
    )

    return ConfigPropertyDefinition(
        property_key=property_name,
        property_type=actual_property_type if not nested_properties else None,
        required=annotation.required if annotation else is_property_required,
        default_value=default_value,
        nested_properties=nested_properties if nested_properties else None,
    )


def _get_config_property_annotation(property_type) -> ConfigPropertyAnnotation | None:
    if get_origin(property_type) is Annotated:
        return next(
            (metadata for metadata in property_type.__metadata__ if isinstance(metadata, ConfigPropertyAnnotation)),
            None,
        )

    return None


def _read_actual_property_type(property_type: type) -> type:
    property_type_origin = get_origin(property_type)

    if property_type_origin is Annotated:
        return property_type.__origin__  # type: ignore[attr-defined]
    elif property_type_origin is Union or property_type_origin is types.UnionType:
        type_args = property_type.__args__  # type: ignore[attr-defined]
        if len(type_args) == 2 and type(None) in type_args:
            # Uses the actual type T when the Union is "T | None" (or "None | T")
            return next(arg for arg in type_args if arg is not None)
        else:
            # Ignoring Union types when it is not used as type | None as we wouldn't which type to pick
            return type(None)

    return property_type


def compute_default_value(
    *, annotation: ConfigPropertyAnnotation | None, property_default: Any | None = None, has_nested_properties: bool
) -> str | None:
    if has_nested_properties:
        return None

    if annotation is not None and annotation.default_value is not None:
        return str(annotation.default_value)

    if property_default is not None:
        return str(property_default)

    return None
