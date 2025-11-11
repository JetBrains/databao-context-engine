from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class EmbeddingItem:
    segment_id: int
    vec: Sequence[float]
