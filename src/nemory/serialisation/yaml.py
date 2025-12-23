from dataclasses import is_dataclass, fields
from enum import Enum
from typing import Any, TextIO, cast

import yaml
from yaml import Node, SafeDumper


# def default_representer(dumper: SafeDumper, data: object) -> Node:
#     if hasattr(data, "__dict__"):
#         # Doesn't serialise "private" attributes (that starts with an _)
#         return dumper.represent_dict({key: value for key, value in data.__dict__.items() if not key.startswith("_")})
#     else:
#         return dumper.represent_str(str(data))
#
#
# # Registers our default representer only once, when that file is imported
# yaml.add_multi_representer(object, default_representer, Dumper=SafeDumper)
#
#
# def write_yaml_to_stream(*, data: Any, file_stream: TextIO) -> None:
#     _to_yaml(data, file_stream)
#
#
# def to_yaml_string(data: Any) -> str:
#     return cast(str, _to_yaml(data, None))
#
#
# def _to_yaml(data: Any, stream: TextIO | None) -> str | None:
#     return yaml.safe_dump(data, stream, sort_keys=False, default_flow_style=False)


def _is_empty(value: Any) -> bool:
    """Treat None, empty strings, and empty containers as 'empty'."""
    if value is None:
        return True
    if isinstance(value, (str, bytes)) and value == "":
        return True
    if isinstance(value, (list, tuple, set, dict)) and len(value) == 0:
        return True
    return False


def default_representer(dumper: SafeDumper, data: object) -> Node:
    if isinstance(data, Enum):
        return dumper.represent_str(str(data.value))

    if is_dataclass(data):
        payload: dict[str, Any] = {}
        for f in fields(data):
            if f.name.startswith("_"):
                continue
            v = getattr(data, f.name)
            keep_empty = f.metadata.get("keep_empty", False)
            if keep_empty or not _is_empty(v):
                payload[f.name] = v
        return dumper.represent_dict(payload)
    if hasattr(data, "__dict__"):
        payload = {
            key: value for key, value in data.__dict__.items() if not key.startswith("_") and not _is_empty(value)
        }
        return dumper.represent_dict(payload)

    return dumper.represent_str(str(data))


yaml.add_multi_representer(object, default_representer, Dumper=SafeDumper)


def write_yaml_to_stream(*, data: Any, file_stream: TextIO) -> None:
    _to_yaml(data, file_stream)


def to_yaml_string(data: Any) -> str:
    return cast(str, _to_yaml(data, None))


def _to_yaml(data: Any, stream: TextIO | None) -> str | None:
    return yaml.safe_dump(data, stream, sort_keys=False, default_flow_style=False, width=240)
