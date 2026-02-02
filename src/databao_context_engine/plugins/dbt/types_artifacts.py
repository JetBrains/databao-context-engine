from dataclasses import dataclass
from typing import Annotated, Literal

from pydantic import BaseModel, Discriminator, Field


class DbtManifestNodeConfig(BaseModel):
    materialized: str


class DbtManifestColumn(BaseModel):
    name: str
    description: str | None = None
    data_type: str | None = None


class DbtManifestModel(BaseModel):
    resource_type: Literal["model"]
    unique_id: str
    name: str
    database: str
    schema_: str = Field(alias="schema")
    description: str | None = None
    config: DbtManifestNodeConfig | None = None
    columns: dict[str, DbtManifestColumn]
    depends_on: dict[str, list[str]] | None = None
    primary_key: list[str] | None = None


class DbtManifestOtherNode(BaseModel):
    resource_type: Literal["seed", "analysis", "test", "operation", "sql_operation", "snapshot"]


DbtManifestNode = Annotated[DbtManifestModel | DbtManifestOtherNode, Discriminator("resource_type")]


class DbtManifest(BaseModel):
    nodes: dict[str, DbtManifestNode]


class DbtCatalogNode(BaseModel):
    unique_id: str


class DbtCatalog(BaseModel):
    nodes: dict[str, DbtCatalogNode]


@dataclass(kw_only=True)
class DbtArtifacts:
    manifest: DbtManifest
    catalog: DbtCatalog | None
