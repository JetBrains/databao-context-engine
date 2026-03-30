from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Any, TextIO, cast

import yaml
from pydantic import BaseModel
from yaml import Node, SafeDumper

from databao_context_engine.datasources.types import DatasourceId
from databao_context_engine.pluginlib.build_plugin import DatasourceType


# Defaults to the string representation of the object for unknown types
def default_representer(dumper: SafeDumper, data: object) -> Node:
    return dumper.represent_str(str(data))


# Registers our default representer only once, when that file is imported
yaml.add_multi_representer(object, default_representer, Dumper=SafeDumper)


def write_yaml_to_stream(*, data: Any, file_stream: TextIO) -> None:
    _to_yaml(data, file_stream)


def to_yaml_string(data: Any) -> str:
    return cast(str, _to_yaml(data, None))


def to_plain_python(value: Any, exclude_none: bool = True) -> Any:
    """Convert any object into a "dictionary representation" of the object.

    All complex objects will be recursively converted to a [attribute_name, attribute_value] dict.

    All values are converted to dicts, list or built-in primitives.
    With two exceptions:
     - objects that only have "slots" fields are returned as-is (e.g. datetime or UUID)
     - objects with no public fields: the string representation of the object is returned

    Args:
        value: The object to be converted
        exclude_none: Whether to exclude fields with None values from the result

    Returns:
        A "dictionary representation" of the object.
    """
    # Special case: BaseModel can directly use Pydantic's "serialisation"
    if isinstance(value, BaseModel):
        return value.model_dump(exclude_none=exclude_none)

    # Special case: DatasourceId and DatasourceType should always be serialised as strings
    if isinstance(value, DatasourceId | DatasourceType):
        return str(value)

    # Write the value for an enum
    if isinstance(value, Enum):
        return value.value

    # Convert dataclasses to a Python dict
    if is_dataclass(value) and not isinstance(value, type):
        return to_plain_python(asdict(value))

    # Handle custom objects by looking at the __dict__ attribute
    # Objects using slots won't be included here and instead will be serialised using their str representation
    if hasattr(value, "__dict__"):
        # Doesn't serialize "private" attributes (that starts with an _) for custom objects
        dict_public_attributes = {key: value for key, value in value.__dict__.items() if not key.startswith("_")}
        if dict_public_attributes:
            return to_plain_python(dict_public_attributes)
        return str(value)

    # Recursively convert dicts
    if isinstance(value, dict):
        return {key: to_plain_python(item) for key, item in value.items() if not exclude_none or item is not None}

    # Handle lists: convert each item in the list
    if isinstance(value, list | tuple | set):
        return [to_plain_python(item) for item in value]

    return value


def _to_yaml(data: Any, stream: TextIO | None) -> str | None:
    return yaml.safe_dump(to_plain_python(data), stream, sort_keys=False, default_flow_style=False, allow_unicode=True)
