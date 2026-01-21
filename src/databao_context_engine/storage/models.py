from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class RunDTO:
    run_id: int
    run_name: str
    project_id: str
    started_at: datetime
    ended_at: Optional[datetime]
    nemory_version: str


@dataclass(frozen=True)
class DatasourceRunDTO:
    datasource_run_id: int
    run_id: int
    plugin: str
    full_type: str
    source_id: str
    storage_directory: str
    created_at: datetime


@dataclass(frozen=True)
class ChunkDTO:
    chunk_id: int
    full_type: str
    datasource_id: str
    display_text: Optional[str]
    created_at: datetime


@dataclass(frozen=True)
class EmbeddingModelRegistryDTO:
    embedder: str
    model_id: str
    dim: int
    table_name: str
    created_at: datetime


@dataclass(frozen=True)
class EmbeddingDTO:
    chunk_id: int
    vec: Sequence[float]
    created_at: datetime
