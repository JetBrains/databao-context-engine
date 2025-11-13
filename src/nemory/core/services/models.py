from collections.abc import Sequence
from dataclasses import dataclass

from nemory.pluginlib.build_plugin import EmbeddableChunk


@dataclass(frozen=True)
class ChunkEmbedding:
    chunk: EmbeddableChunk
    vec: Sequence[float]
