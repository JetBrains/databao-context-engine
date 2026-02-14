from dataclasses import dataclass
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Discriminator, Field


class DbtManifestModelConfig(BaseModel):
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
    config: DbtManifestModelConfig | None = None
    columns: dict[str, DbtManifestColumn]
    depends_on: dict[str, list[str]] | None = None
    primary_key: list[str] | None = None


class DbtManifestTestConfig(BaseModel):
    severity: str


class DbtManifestTestMetadata(BaseModel):
    name: str | None = None
    kwargs: dict[str, Any] | None = None


class DbtManifestTest(BaseModel):
    resource_type: Literal["test"]
    unique_id: str
    attached_node: str | None = None
    column_name: str | None = None
    description: str | None = None
    test_metadata: DbtManifestTestMetadata | None = None
    config: DbtManifestTestConfig | None = None


class DbtManifestOtherNode(BaseModel):
    resource_type: Literal["seed", "analysis", "operation", "sql_operation", "snapshot"]


DbtManifestNode = Annotated[DbtManifestModel | DbtManifestTest | DbtManifestOtherNode, Discriminator("resource_type")]


class DbtManifestSemanticEntity(BaseModel):
    name: str
    type: Literal["foreign", "natural", "primary", "unique"]
    description: str | None = None


class DbtManifestSemanticMeasure(BaseModel):
    name: str
    agg: Literal["sum", "min", "max", "count_distinct", "sum_boolean", "average", "percentile", "median", "count"]
    description: str | None = None


class DbtManifestSemanticDimension(BaseModel):
    name: str
    type: Literal["time", "categorical"]
    description: str | None = None


class DbtManifestSemanticModel(BaseModel):
    name: str
    resource_type: Literal["semantic_model"]
    unique_id: str
    model: str
    description: str | None = None
    entities: list[DbtManifestSemanticEntity] | None = None
    measures: list[DbtManifestSemanticMeasure] | None = None
    dimensions: list[DbtManifestSemanticDimension] | None = None


class DbtManifestMetric(BaseModel):
    name: str
    resource_type: Literal["metric"]
    unique_id: str
    description: str
    type: Literal["simple", "ratio", "cumulative", "derived", "conversion"]
    label: str
    depends_on: dict[str, list[str]] | None = None


class DbtManifest(BaseModel):
    nodes: dict[str, DbtManifestNode]
    semantic_models: dict[str, DbtManifestSemanticModel]
    metrics: dict[str, DbtManifestMetric]


class DbtCatalogColumn(BaseModel):
    name: str
    type: str


class DbtCatalogNode(BaseModel):
    unique_id: str | None = None
    columns: dict[str, DbtCatalogColumn]


class DbtCatalog(BaseModel):
    nodes: dict[str, DbtCatalogNode]


@dataclass(kw_only=True)
class DbtArtifacts:
    manifest: DbtManifest
    catalog: DbtCatalog | None
