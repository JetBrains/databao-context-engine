import types
from dataclasses import MISSING, fields, is_dataclass
from typing import (
    Annotated,
    Any,
    ForwardRef,
    Iterable,
    Mapping,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

from pydantic import BaseModel
from pydantic_core import PydanticUndefinedType

from databao_context_engine.pluginlib.config import (
    ConfigPropertyAnnotation,
    ConfigPropertyDefinition,
    ConfigSinglePropertyDefinition,
    ConfigUnionPropertyDefinition,
)


def get_property_list_from_type(root_type: type) -> list[ConfigPropertyDefinition]:
    return _get_property_list_from_type(parent_type=root_type)


def _get_property_list_from_type(*, parent_type: type) -> list[ConfigPropertyDefinition]:
    if is_dataclass(parent_type):
        return _get_property_list_from_dataclass(parent_type=parent_type)

    try:
        if issubclass(parent_type, BaseModel):
            return _get_property_list_from_pydantic_base_model(parent_type=parent_type)
    except TypeError:
        # when trying to compare ABC Metadata classes to BaseModel, e.g: issubclass(Mapping[str, str], BaseModel)
        # issubclass is raising a TypeError: issubclass() arg 1 must be a class
        pass

    return _get_property_list_from_type_hints(parent_type=parent_type)


def _get_property_list_from_type_hints(*, parent_type: type) -> list[ConfigPropertyDefinition]:
    try:
        type_hints = get_type_hints(parent_type, include_extras=True)
    except TypeError:
        # Return an empty list of properties for any type that is not an object (e.g: primitives like str or containers like dict, list, tuple, etc.
        return []

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
    type_hints = get_type_hints(parent_type, include_extras=True)

    result = []
    for field in dataclass_fields:
        has_field_default = field.default != MISSING

        # Use the type hints if the field type wasn't resolved (aka. if it is a ForwardRef or a str)
        property_type = type_hints[field.name] if isinstance(field.type, ForwardRef | str) else field.type

        property_for_field = _create_property(
            property_type=property_type,
            property_name=field.name,
            property_default=field.default if has_field_default else None,
            is_property_required=not has_field_default,
        )

        if property_for_field is not None:
            result.append(property_for_field)

    return result


def _get_property_list_from_pydantic_base_model(parent_type: type):
    if not issubclass(parent_type, BaseModel):
        raise ValueError(f"{parent_type} is not a Pydantic BaseModel")

    if any(isinstance(field.annotation, ForwardRef) for field in parent_type.model_fields.values()):
        # If any field's future type wasn't resolved yet, we rebuild the model to resolve them
        parent_type.model_rebuild(force=True)

    pydantic_fields = parent_type.model_fields
    result = []

    for field_name, field_info in pydantic_fields.items():
        has_field_default = type(field_info.default) is not PydanticUndefinedType
        has_default_factory = field_info.default_factory is not None

        if field_info.annotation is None:
            # No type: ignore the field
            continue

        if has_field_default:
            resolved_default = field_info.default
        elif has_default_factory:
            resolved_default = field_info.default_factory()  # type: ignore[call-arg,misc]
        else:
            resolved_default = None

        property_for_field = _create_property(
            property_type=field_info.annotation,
            property_name=field_name,
            property_default=resolved_default,
            is_property_required=not (has_field_default or has_default_factory),
            annotation=next(
                (metadata for metadata in field_info.metadata if isinstance(metadata, ConfigPropertyAnnotation)), None
            ),
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
    annotation: ConfigPropertyAnnotation | None = None,
) -> ConfigPropertyDefinition | None:
    annotation = annotation or _get_config_property_annotation(property_type)

    if annotation is not None and annotation.ignored_for_config_wizard:
        return None

    actual_property_types = _read_actual_property_type(property_type)

    required = annotation.required if annotation else is_property_required
    secret = annotation.secret if annotation else False

    if len(actual_property_types) > 1:
        type_properties: dict[type, list[ConfigPropertyDefinition]] = {}

        for union_type in actual_property_types:
            nested_props = _get_property_list_from_type(parent_type=union_type)

            type_properties[union_type] = nested_props

        default_type = type(property_default) if property_default is not None else None
        if default_type is not None and default_type not in actual_property_types:
            default_type = None

        return ConfigUnionPropertyDefinition(
            property_key=property_name,
            types=actual_property_types,
            type_properties=type_properties,
            default_type=default_type,
        )

    actual_property_type = actual_property_types[0]
    nested_properties = _get_property_list_from_type(parent_type=actual_property_type)

    if len(nested_properties) == 0 and _is_mapping_or_iterable(actual_property_type):
        # Ignore Iterables and Mappings for which we didn't resolve nested properties
        # (TypedDict is a Mapping but since we manage to resolve nested properties, it won't be ignored)
        return None

    resolved_type = actual_property_type if not nested_properties else None
    default_value = compute_default_value(
        annotation=annotation,
        property_default=property_default,
        has_nested_properties=nested_properties is not None and len(nested_properties) > 0,
    )

    return ConfigSinglePropertyDefinition(
        property_key=property_name,
        property_type=resolved_type,
        required=required,
        default_value=default_value,
        nested_properties=nested_properties or None,
        secret=secret,
    )


def _is_mapping_or_iterable(property_type: type):
    # For types like list[str], we need to get the origin (ie. list) to use in issubclass
    origin = get_origin(property_type)

    try:
        # We make sure to not return True for str, which is an Iterable
        return property_type is not str and issubclass(origin if origin else property_type, (Mapping, Iterable))
    except TypeError:
        # Special typing forms like Literal are not classes; issubclass raises TypeError
        return False


def _get_config_property_annotation(property_type) -> ConfigPropertyAnnotation | None:
    if get_origin(property_type) is Annotated:
        return next(
            (metadata for metadata in property_type.__metadata__ if isinstance(metadata, ConfigPropertyAnnotation)),
            None,
        )

    return None


def _read_actual_property_type(property_type: type) -> tuple[type, ...]:
    property_type_origin = get_origin(property_type)

    if property_type_origin is Annotated:
        return _read_actual_property_type(property_type.__origin__)  # type: ignore[attr-defined]
    if property_type_origin in (Union, types.UnionType):
        return tuple(arg for arg in get_args(property_type) if arg is not type(None))

    return (property_type,)


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
