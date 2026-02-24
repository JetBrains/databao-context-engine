from collections.abc import Sequence
from dataclasses import dataclass

from databao_context_engine.pluginlib.build_plugin import EmbeddableChunk


@dataclass(frozen=True)
class ChunkEmbedding:
    original_chunk: EmbeddableChunk
    vec: Sequence[float]
    embedded_text: str
    display_text: str
    generated_description: str
