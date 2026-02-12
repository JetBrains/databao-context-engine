from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from databao_context_engine.pluginlib.build_plugin import AbstractConfigFile


class DbtConfigFile(BaseModel, AbstractConfigFile):
    name: str
    type: str = Field(default="dbt")
    dbt_target_folder_path: Path


class DbtMaterialization(str, Enum):
    TABLE = "table"
    VIEW = "view"

    def __str__(self):
        return self.value


@dataclass(kw_only=True)
class DbtSimpleConstraint:
    type: Literal["unique", "not_null"]
    is_enforced: bool
    description: str | None = None


@dataclass(kw_only=True)
class DbtAcceptedValuesConstraint:
    type: Literal["accepted_values"]
    is_enforced: bool
    description: str | None = None
    accepted_values: list[str]


@dataclass(kw_only=True)
class DbtRelationshipConstraint:
    type: Literal["relationships"]
    is_enforced: bool
    description: str | None = None
    target_model: str
    target_column: str


DbtConstraint = DbtSimpleConstraint | DbtAcceptedValuesConstraint | DbtRelationshipConstraint


@dataclass(kw_only=True)
class DbtColumn:
    name: str
    type: str | None = None
    description: str | None = None
    constraints: list[DbtConstraint] | None = None


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
