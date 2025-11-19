from dataclasses import dataclass
from pathlib import Path
from typing import Any

from nemory.pluginlib.build_plugin import BuildPlugin

PluginList = dict[str, BuildPlugin]


@dataclass(frozen=True)
class PreparedConfig:
    full_type: str
    path: Path
    config: dict[Any, Any]
    datasource_name: str


@dataclass(frozen=True)
class PreparedFile:
    full_type: str
    path: Path


PreparedDatasource = PreparedConfig | PreparedFile
