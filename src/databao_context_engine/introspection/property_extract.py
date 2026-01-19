import types
from dataclasses import MISSING, fields, is_dataclass
from typing import Annotated, Any, ForwardRef, Union, get_origin, get_type_hints

from pydantic import BaseModel, _internal
from pydantic_core import PydanticUndefinedType

from databao_context_engine.pluginlib.config import ConfigPropertyAnnotation, ConfigPropertyDefinition


def get_property_list_from_type(root_type: type) -> list[ConfigPropertyDefinition]:
    return _get_property_list_from_type(parent_type=root_type, is_root_type=True)


def _get_property_list_from_type(*, parent_type: type, is_root_type: bool) -> list[ConfigPropertyDefinition]:
    if is_dataclass(parent_type):
        return _get_property_list_from_dataclass(parent_type=parent_type)

    try:
        if issubclass(parent_type, BaseModel):
            return _get_property_list_from_pydantic_base_model(parent_type=parent_type)
    except TypeError:
        # when trying to compare ABC Metadata classes to BaseModel, e.g: issubclass(Mapping[str, str], BaseModel)
        # issubclass is raising a TypeError: issubclass() arg 1 must be a class
        pass

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

        if isinstance(field.type, str):
            try:
                property_type = _evaluate_type_string(field.type)
            except Exception:
                continue
        else:
            property_type = field.type

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

    pydantic_fields = parent_type.model_fields
    result = []

    for field_name, field_info in pydantic_fields.items():
        has_field_default = type(field_info.default) is not PydanticUndefinedType

        if field_info.annotation is None:
            # No type: ignore the field
            continue

        property_for_field = _create_property(
            property_type=field_info.annotation,
            property_name=field_name,
            property_default=field_info.default if has_field_default else None,
            is_property_required=not has_field_default,
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
        secret=annotation.secret if annotation else False,
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


def _evaluate_type_string(property_type: str) -> type:
    try:
        # Using a pydantic internal function for this, to avoid having to implement type evaluation manually...
        return _internal._typing_extra.eval_type(property_type)
    except Exception as initial_error:
        try:
            # Try to convert it ourselves if Pydantic didn't work
            return ForwardRef(property_type)._evaluate(  # type: ignore[return-value]
                globalns=globals(), localns=locals(), recursive_guard=frozenset()
            )
        except Exception as e:
            # Ignore if we didn't manage to convert the str to a type
            raise e from initial_error
