from collections.abc import Sequence
from dataclasses import dataclass

from databao_context_engine.pluginlib.build_plugin import EmbeddableChunk


@dataclass(frozen=True)
class ChunkEmbedding:
    chunk: EmbeddableChunk
    vec: Sequence[float]
    display_text: str
    generated_description: str
