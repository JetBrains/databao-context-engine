from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass(kw_only=True)
class ConfigPropertyDefinition:
    property_key: str
    required: bool
    property_type: type | None = str
    default_value: str | None = None
    nested_properties: list["ConfigPropertyDefinition"] | None = None


@runtime_checkable
class CustomiseConfigProperties(Protocol):
    def get_config_file_properties(self) -> list[ConfigPropertyDefinition]: ...
