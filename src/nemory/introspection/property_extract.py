import types
from typing import Annotated, Union, get_origin, get_type_hints

from nemory.pluginlib.config_properties import ConfigPropertyAnnotation, ConfigPropertyDefinition


def get_property_list_from_type(root_type: type) -> list[ConfigPropertyDefinition]:
    return _get_property_list_from_type(parent_type=root_type, is_root_type=True)


def _get_property_list_from_type(*, parent_type: type, is_root_type: bool) -> list[ConfigPropertyDefinition]:
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
        property_type_origin = get_origin(property_type)
        annotation = None
        if property_type_origin is Annotated:
            annotation = next(
                (metadata for metadata in property_type.__metadata__ if isinstance(metadata, ConfigPropertyAnnotation)),
                None,
            )
            property_type = property_type.__origin__
        elif property_type_origin is Union or property_type_origin is types.UnionType:
            type_args = property_type.__args__
            if len(type_args) == 2 and type(None) in type_args:
                # Uses the actual type T when the Union is "T | None" (or "None | T")
                property_type = next(arg for arg in type_args if arg is not None)
            else:
                # Ignoring Union types when it is not used as type | None as we wouldn't which type to pick
                property_type = type(None)

        if annotation is not None and annotation.ignored_for_config_wizard:
            continue

        try:
            nested_properties = _get_property_list_from_type(parent_type=property_type, is_root_type=False)
        except TypeError:
            continue

        result.append(
            ConfigPropertyDefinition(
                property_key=property_key,
                property_type=property_type if not nested_properties else None,
                required=annotation.required if annotation else False,
                default_value=str(annotation.default_value)
                if annotation is not None and annotation.default_value is not None
                else None,
                nested_properties=nested_properties if nested_properties else None,
            )
        )

    return result
