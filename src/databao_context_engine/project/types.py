from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from databao_context_engine.pluginlib.build_plugin import DatasourceType


class DatasourceKind(StrEnum):
    CONFIG = "config"
    FILE = "file"


@dataclass(frozen=True)
class DatasourceDescriptor:
    path: Path
    kind: DatasourceKind
    main_type: str


@dataclass(frozen=True)
class PreparedConfig:
    datasource_type: DatasourceType
    path: Path
    config: dict[Any, Any]
    datasource_name: str


@dataclass(frozen=True)
class PreparedFile:
    datasource_type: DatasourceType
    path: Path


PreparedDatasource = PreparedConfig | PreparedFile
