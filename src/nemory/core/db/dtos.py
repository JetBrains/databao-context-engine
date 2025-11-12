from datetime import datetime
from enum import Enum
from dataclasses import dataclass
from typing import Optional
from collections.abc import Sequence


class RunStatus(str, Enum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class RunDTO:
    run_id: int
    status: RunStatus
    project_id: str
    started_at: datetime
    ended_at: Optional[datetime]
    nemory_version: str


@dataclass(frozen=True)
class EntityDTO:
    entity_id: int
    run_id: int
    plugin: str
    source_id: str
    storage_directory: str
    created_at: datetime


@dataclass(frozen=True)
class SegmentDTO:
    segment_id: int
    entity_id: int
    embeddable_text: str
    display_text: Optional[str]
    created_at: datetime


@dataclass(frozen=True)
class EmbeddingDTO:
    segment_id: int
    embedder: str
    model_id: str
    vec: Sequence[float]
    created_at: datetime
