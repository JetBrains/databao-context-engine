from typing import Any, TextIO, cast

import yaml
from yaml import Node, SafeDumper


def default_representer(dumper: SafeDumper, data: object) -> Node:
    if hasattr(data, "__dict__"):
        # Doesn't serialise "private" attributes (that starts with an _)
        return dumper.represent_dict({key: value for key, value in data.__dict__.items() if not key.startswith("_")})
    else:
        return dumper.represent_str(str(data))


# Registers our default representer only once, when that file is imported
yaml.add_multi_representer(object, default_representer, Dumper=SafeDumper)


def write_yaml_to_stream(*, data: Any, file_stream: TextIO) -> None:
    _to_yaml(data, file_stream)


def to_yaml_string(data: Any) -> str:
    return cast(str, _to_yaml(data, None))


def _to_yaml(data: Any, stream: TextIO | None) -> str | None:
    return yaml.safe_dump(data, stream, sort_keys=False, default_flow_style=False)
