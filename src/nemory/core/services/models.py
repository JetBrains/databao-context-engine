from collections.abc import Sequence
from dataclasses import dataclass

from nemory.features.build_sources.plugin_lib.build_plugin import EmbeddableChunk


@dataclass(frozen=True)
class ChunkEmbedding:
    chunk: EmbeddableChunk
    vec: Sequence[float]
