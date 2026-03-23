from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from databao_context_engine.pluginlib.config import (
    ConfigPropertyDefinition,
    ConfigUnionPropertyDefinition,
)
from databao_context_engine.project.project_secrets import make_secret_ref, parse_secret_ref

_SECRET_KEY_PART_PATTERN = re.compile(r"[^A-Za-z0-9_-]+")
_ENV_VAR_TEMPLATE_PATTERN = re.compile(r"^\s*\{\{\s*env_var\(.+\)\s*\}\}\s*$")


@dataclass(frozen=True)
class SecretExtractionResult:
    config_with_secret_refs: dict[str, Any]
    secrets: dict[str, Any]


@dataclass(frozen=True)
class SecretField:
    parent_node: dict[str, Any]
    field_name: str
    field_path: tuple[str, ...]
    value: Any


def extract_secrets_from_config(
    config_content: dict[str, Any],
    properties: list[ConfigPropertyDefinition],
    datasource_relative_path: str,
) -> SecretExtractionResult:
    """Replace secret values in a config with secret refs and collect the extracted values."""
    config_with_secret_refs = deepcopy(config_content)
    secrets: dict[str, Any] = {}
    secret_key_prefix = build_secret_key_prefix(datasource_relative_path)

    for secret_field in _iter_secret_fields(config_with_secret_refs, properties):
        secret_key = _make_secret_key(secret_key_prefix, secret_field.field_path)
        secrets[secret_key] = secret_field.value
        secret_field.parent_node[secret_field.field_name] = make_secret_ref(secret_key)

    return SecretExtractionResult(
        config_with_secret_refs=config_with_secret_refs,
        secrets=secrets,
    )


def _iter_secret_fields(
    node: dict[str, Any],
    properties: list[ConfigPropertyDefinition],
    field_path: tuple[str, ...] = (),
) -> Iterator[SecretField]:
    """Yield secret leaf fields found while walking a config node and its nested properties."""
    for property_definition in properties:
        field_name = property_definition.property_key
        if field_name not in node:
            continue

        value = node[field_name]
        next_field_path = (*field_path, field_name)

        if isinstance(property_definition, ConfigUnionPropertyDefinition):
            if isinstance(value, dict):
                branch_properties = _get_union_branch_properties(property_definition, value)
                yield from _iter_secret_fields(value, branch_properties, next_field_path)
            continue

        if property_definition.nested_properties:
            if isinstance(value, dict):
                yield from _iter_secret_fields(
                    value,
                    property_definition.nested_properties,
                    next_field_path,
                )
            continue

        if property_definition.secret and value is not None and not _is_reference_value(value):
            yield SecretField(
                parent_node=node,
                field_name=field_name,
                field_path=next_field_path,
                value=value,
            )


def build_secret_key_prefix(datasource_relative_path: str) -> str:
    """Build the stable secret key prefix for a datasource from its relative config path."""
    datasource_path = Path(datasource_relative_path)
    path_parts = list(datasource_path.parts)
    if not path_parts:
        return ""

    path_parts[-1] = Path(path_parts[-1]).stem or path_parts[-1]

    return ".".join(_sanitize_secret_key_part(part) for part in path_parts if part)


def _get_union_branch_properties(
    union_property: ConfigUnionPropertyDefinition,
    union_value: Mapping[str, Any],
) -> list[ConfigPropertyDefinition]:
    """Return the active union branch properties based on the `type` field or the default branch.

    Secret extraction needs to know which union variant is currently in use so it can continue walking the correct
    nested fields. The branch is selected from the union's `type` field when present, otherwise the default branch is
    used.

    Returns:
        The property definitions for the selected union branch. Returns an empty list when the union has no `type` value
        and no default branch is defined.

    Raises:
        ValueError: If the union value contains a `type` that is not known by the union property definition.
    """
    union_type = union_value.get("type")

    if isinstance(union_type, str):
        type_properties = cast(dict[str, list[ConfigPropertyDefinition]], union_property.type_properties)
        branch_properties = type_properties.get(union_type)
        if branch_properties is None:
            raise ValueError(f"Unknown union type: {union_type!r}")
        return branch_properties

    if union_property.default_type is None:
        return []

    return union_property.type_properties[union_property.default_type]


def _make_secret_key(secret_key_prefix: str, field_path: tuple[str, ...]) -> str:
    normalized_field_path = [_sanitize_secret_key_part(field_name) for field_name in field_path if field_name != "type"]
    return ".".join(part for part in [secret_key_prefix, *normalized_field_path] if part)


def _sanitize_secret_key_part(value: str) -> str:
    sanitized_value = _SECRET_KEY_PART_PATTERN.sub("_", str(value).strip()).strip("_")
    return sanitized_value or "value"


def _is_reference_value(value: Any) -> bool:
    if not isinstance(value, str):
        return False

    return parse_secret_ref(value) is not None or _ENV_VAR_TEMPLATE_PATTERN.fullmatch(value) is not None
