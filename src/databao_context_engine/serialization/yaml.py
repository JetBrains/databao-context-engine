from dataclasses import fields, is_dataclass
from typing import Any, Mapping, TextIO, cast

import yaml
from pydantic import BaseModel
from yaml import Node, SafeDumper


def default_representer(dumper: SafeDumper, data: object) -> Node:
    if isinstance(data, Mapping):
        return dumper.represent_dict(data)

    if is_dataclass(data) and not isinstance(data, type):
        ordered: dict[str, Any] = {}
        for field in fields(data):
            ordered[field.name] = getattr(data, field.name)
        return dumper.represent_dict(ordered)

    if BaseModel is not None and isinstance(data, BaseModel):
        ordered: dict[str, Any] = {}
        for name in data.model_fields.keys():
            ordered[name] = getattr(data, name)
        return dumper.represent_dict(ordered)

    if hasattr(data, "__dict__"):
        # Doesn't serialize "private" attributes (that starts with an _)
        data_public_attributes = {key: value for key, value in data.__dict__.items() if not key.startswith("_")}
        if data_public_attributes:
            ordered = {key: data_public_attributes[key] for key in sorted(data_public_attributes)}
            return dumper.represent_dict(ordered)

        # If there is no public attributes, we default to the string representation
        return dumper.represent_str(str(data))

    return dumper.represent_str(str(data))


# Registers our default representer only once, when that file is imported
yaml.add_multi_representer(object, default_representer, Dumper=SafeDumper)


def write_yaml_to_stream(*, data: Any, file_stream: TextIO) -> None:
    _to_yaml(data, file_stream)


def to_yaml_string(data: Any) -> str:
    return cast(str, _to_yaml(data, None))


def _to_yaml(data: Any, stream: TextIO | None) -> str | None:
    return yaml.safe_dump(data, stream, sort_keys=False, default_flow_style=False)
