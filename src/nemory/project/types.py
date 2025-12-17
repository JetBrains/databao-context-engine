from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any


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
    full_type: str
    path: Path
    config: dict[Any, Any]
    datasource_name: str


@dataclass(frozen=True)
class PreparedFile:
    full_type: str
    path: Path


class PreparedDatasourceError(Exception):
    path: Path

    def __init__(self, message: str, path: Path, cause: Exception | None = None):
        self.__cause__ = cause
        super().__init__(message)

        self.path = path


PreparedDatasource = PreparedConfig | PreparedFile
