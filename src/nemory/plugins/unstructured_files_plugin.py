import re
import uuid
from datetime import datetime
from io import BufferedReader
from typing import TypedDict

from nemory.pluginlib.build_plugin import BuildFilePlugin, BuildExecutionResult, EmbeddableChunk


class FileChunk(TypedDict):
    chunk_index: int
    chunk_content: str


class InternalUnstructuredFilesPlugin(BuildFilePlugin):
    name = "Unstructured Files Plugin"

    _SUPPORTED_FILES_EXTENSIONS = {"txt", "md"}

    _DEFAULT_MAX_TOKENS = 300
    _DEFAULT_TOKENS_OVERLAP = 50

    def __init__(self, max_tokens: int | None = None, tokens_overlap: int | None = None):
        self.max_tokens = max_tokens or self._DEFAULT_MAX_TOKENS
        self.tokens_overlap = tokens_overlap or self._DEFAULT_TOKENS_OVERLAP

    def supported_types(self) -> set[str]:
        return {f"files/{extension}" for extension in self._SUPPORTED_FILES_EXTENSIONS}

    def execute(self, full_type: str, file_name: str, file_buffer: BufferedReader) -> BuildExecutionResult:
        file_content = self._read_file(file_buffer)

        return BuildExecutionResult(
            id=str(uuid.uuid4()),
            name=file_name,
            type=full_type,
            description="",
            version="1.0.0",
            executed_at=datetime.now(),
            result={
                "chunks": self._chunk_file(file_content),
            },
        )

    def divide_result_into_chunks(self, build_result: BuildExecutionResult) -> list[EmbeddableChunk]:
        return [
            self._create_embeddable_chunk_from_file_chunk(file_chunk) for file_chunk in build_result.result["chunks"]
        ]

    def _create_embeddable_chunk_from_file_chunk(self, file_chunk: FileChunk) -> EmbeddableChunk:
        return EmbeddableChunk(
            embeddable_text=file_chunk["chunk_content"],
            content=file_chunk,
        )

    def _read_file(self, file_buffer: BufferedReader) -> str:
        file_bytes = file_buffer.read()
        return file_bytes.decode("utf-8")

    def _chunk_file(self, file_content: str) -> list[FileChunk]:
        words_list = re.split(r"\s+", file_content)

        chunks = []

        chunk_start_index = 0
        number_of_words = len(words_list)
        while chunk_start_index < number_of_words:
            chunk_end_index = min(number_of_words, chunk_start_index + self.max_tokens)
            chunks.append(
                FileChunk(
                    chunk_index=chunk_start_index,
                    chunk_content=" ".join(words_list[chunk_start_index:chunk_end_index]),
                )
            )
            chunk_start_index = (
                (chunk_end_index - self.tokens_overlap) if chunk_end_index < number_of_words else number_of_words
            )

        return chunks
