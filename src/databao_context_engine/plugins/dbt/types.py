from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field


class DbtConfigFile(BaseModel):
    name: str | None = Field(default=None)
    type: str = Field(default="dbt")
    dbt_target_folder_path: Path


class DbtMaterialization(str, Enum):
    TABLE = "table"
    VIEW = "view"

    def __str__(self):
        return self.value


@dataclass(kw_only=True)
class DbtColumn:
    name: str
    type: str | None = None
    description: str | None = None


@dataclass(kw_only=True)
class DbtModel:
    id: str
    name: str
    database: str
    schema: str
    columns: list[DbtColumn]
    description: str | None = None
    materialization: DbtMaterialization | None = None
    primary_key: list[str] | None = None
    depends_on_nodes: list[str]


@dataclass(kw_only=True)
class DbtContext:
    models: list[DbtModel]
