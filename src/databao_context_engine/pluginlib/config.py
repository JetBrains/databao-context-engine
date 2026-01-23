from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, Field


class DuckDBSecret(BaseModel):
    name: str | None = Field(default=None)
    type: str = Field(
        description="DuckDB secret type. Examples: s3, postgres, iceberg, etc. See https://duckdb.org/docs/stable/configuration/secrets_manager#types-of-secrets"
    )
    properties: dict[str, Any] = Field(
        default={},
        description="Key/Value pairs which will be used to create a duckdb secret. "
        "See https://duckdb.org/docs/stable/configuration/secrets_manager",
    )


@dataclass(kw_only=True)
class ConfigUnionPropertyDefinition:
    property_key: str
    types: tuple[type, ...]
    type_properties: dict[type, list[ConfigPropertyDefinition]]


@dataclass(kw_only=True)
class ConfigSinglePropertyDefinition:
    property_key: str
    required: bool
    property_type: type | None = str
    default_value: str | None = None
    nested_properties: list[ConfigPropertyDefinition] | None = None
    secret: bool = False


ConfigPropertyDefinition = ConfigSinglePropertyDefinition | ConfigUnionPropertyDefinition


@dataclass(kw_only=True)
class ConfigPropertyAnnotation:
    required: bool = False
    default_value: str | None = None
    ignored_for_config_wizard: bool = False
    secret: bool = False


@runtime_checkable
class CustomiseConfigProperties(Protocol):
    def get_config_file_properties(self) -> list[ConfigPropertyDefinition]: ...
