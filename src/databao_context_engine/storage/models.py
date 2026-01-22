from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class ChunkDTO:
    chunk_id: int
    full_type: str
    datasource_id: str
    embeddable_text: str
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
