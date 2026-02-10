from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from docling_core.transforms.chunker import HybridChunker
from docling_core.transforms.chunker.tokenizer.huggingface import HuggingFaceTokenizer
from docling_core.types.doc.labels import DocItemLabel

from databao_context_engine.pluginlib.build_plugin import EmbeddableChunk


@dataclass(frozen=True)
class EmbeddingPolicy:
    """Configuration for embedding oriented chunking.

    Attributes:
            model_name: Tokenizer/model identifier used for token counting
            tokens_budget: Max number of tokens allowed in each chunk
            tokens_overlap: Overlap between consecutive splits to reduce boundary loss
            tokenizer_unbounded_max: a very large value used to suppress HF tokenizer warnings when encoding long texts
            hard_truncate_chars: last resert char cap for single rows
    """

    model_name: str = "nomic-ai/nomic-embed-text-v1.5"
    tokens_budget: int = 1900
    tokens_overlap: int = 80

    tokenizer_unbounded_max: int = 1_000_000_000

    hard_truncate_chars: int = 6000


class TokenSplitter:
    def __init__(self, policy: EmbeddingPolicy):
        self.policy = policy

    def split(self, text: str, *, tokenizer: HuggingFaceTokenizer) -> list[str]:
        """Return `text` split into parts that fit within the policy token budget.

        Returns:
            A list of strings. If `text` is empty, returns [""]

        Behavior:
            - If `text` fits the budget, returns [text]
            - Otherwise tokenizes and slices into windows of size `tokens_budget` with overlap `tokens_overlap`
        """
        if not text:
            return [""]

        if tokenizer.count_tokens(text) <= self.policy.tokens_budget:
            return [text]

        hf = tokenizer.get_tokenizer()
        hf.model_max_length = self.policy.tokenizer_unbounded_max

        ids = hf.encode(text, add_special_tokens=False)
        if not ids:
            return [text]

        step = max(1, self.policy.tokens_budget - self.policy.tokens_overlap)
        parts: list[str] = []

        for start in range(0, len(ids), step):
            end = min(len(ids), start + self.policy.tokens_budget)
            parts.append(hf.decode(ids[start:end]))
            if end == len(ids):
                break

        return parts or [text]


class DoclingChunker:
    """Convert a Docling document into EmbeddableChunks.

    Pipeline:
        1. Create a Docling `HybridChunker` for structural chunking.
        2. For non-table chunks: contextualize and token-split into embeddable parts.
        3. For tables: chunk separately.
            - Fast path: embed caption + markdown if it fits
            - Slow path: export to dataframe and row-batch without losing rows
    """

    def __init__(self, policy: Optional[EmbeddingPolicy] = None):
        self.policy = policy or EmbeddingPolicy()
        self.splitter = TokenSplitter(self.policy)

    def index(self, doc: Any) -> list[EmbeddableChunk]:
        """Create embeddable chunks from a Docling document.

        Returns:
            A list of EmbeddableChunks.
        """
        tokenizer = self._make_tokenizer()
        chunker = self._make_chunker(tokenizer)

        chunks: list[EmbeddableChunk] = []
        chunks.extend(self._index_text(doc=doc, chunker=chunker, tokenizer=tokenizer))
        chunks.extend(self._index_tables(doc=doc, tokenizer=tokenizer))
        return chunks

    def _make_tokenizer(self) -> HuggingFaceTokenizer:
        return HuggingFaceTokenizer.from_pretrained(
            model_name=self.policy.model_name,
            max_tokens=self.policy.tokens_budget,
        )

    def _make_chunker(self, tokenizer: HuggingFaceTokenizer) -> HybridChunker:
        return HybridChunker(tokenizer=tokenizer, merge_peers=False)

    def _index_text(
        self,
        *,
        doc: Any,
        chunker: HybridChunker,
        tokenizer: HuggingFaceTokenizer,
    ) -> list[EmbeddableChunk]:
        """Index non-table text chunks produced by Docling's chunker."""
        out: list[EmbeddableChunk] = []

        for chunk in chunker.chunk(dl_doc=doc):
            if self._is_table_chunk(chunk):
                continue

            raw_text = getattr(chunk, "text", "")
            display_text = (raw_text or "").strip()
            if not display_text:
                continue

            embed_text = chunker.contextualize(chunk=chunk)

            if tokenizer.count_tokens(embed_text) <= self.policy.tokens_budget:
                out.append(EmbeddableChunk(embeddable_text=embed_text, content=display_text))
                continue

            delim = getattr(chunker, "delim", "\n")
            if delim in embed_text:
                header, body = embed_text.rsplit(delim, 1)
                header = header.strip()
                body = body.strip()

                if not body or not header:
                    parts = self.splitter.split(embed_text, tokenizer=tokenizer)
                else:
                    parts = []
                    for body_part in self.splitter.split(body, tokenizer=tokenizer):
                        candidate = f"{header}{delim}{body_part}".strip()
                        parts.append(candidate)

            else:
                parts = self.splitter.split(embed_text, tokenizer=tokenizer)

            for part in parts:
                out.append(EmbeddableChunk(embeddable_text=part, content=display_text))

        return out

    def _is_table_chunk(self, chunk: Any) -> bool:
        """Return True if the Docling chunk references a table item."""
        meta = getattr(chunk, "meta", None)
        doc_items = getattr(meta, "doc_items", None) or []

        for it in doc_items:
            label = getattr(it, "label", None)
            if label == DocItemLabel.TABLE:
                return True

            ref = getattr(it, "self_ref", None)
            if isinstance(ref, str) and "/tables/" in ref:
                return True

        return False

    def _index_tables(self, *, doc: Any, tokenizer: HuggingFaceTokenizer) -> list[EmbeddableChunk]:
        """Index all the tables in the document using the table-specific strategy."""
        out: list[EmbeddableChunk] = []
        for table in getattr(doc, "tables", None) or []:
            out.extend(self._table_to_chunks(table=table, doc=doc, tokenizer=tokenizer))
        return out

    def _table_to_chunks(self, *, table: Any, doc: Any, tokenizer: HuggingFaceTokenizer) -> list[EmbeddableChunk]:
        """Convert a single table into one or more embeddable chunks."""
        table_md = table.export_to_markdown(doc=doc)
        caption = self._get_table_caption(table)

        fast_embed = "\n".join([line for line in [f"TABLE: {caption}".strip(), table_md] if line.strip()])
        if tokenizer.count_tokens(fast_embed) <= self.policy.tokens_budget:
            return [EmbeddableChunk(embeddable_text=fast_embed, content=table_md)]

        df = table.export_to_dataframe(doc=doc)
        headers = [str(c) for c in df.columns]
        rows = df.astype(str).values.tolist()

        prefix = self._format_table_prefix(caption=caption, headers=headers)
        base = prefix + "\nROWS:\n"

        chunks: list[EmbeddableChunk] = []
        current: list[str] = []

        for r in rows:
            line = "- " + " | ".join((c or "").strip() for c in r)
            candidate = base + "\n".join(current + [line])

            if tokenizer.count_tokens(candidate) > self.policy.tokens_budget:
                if current:
                    chunks.append(EmbeddableChunk(embeddable_text=base + "\n".join(current), content=table_md))
                    current = [line]
                else:
                    chunks.append(
                        EmbeddableChunk(
                            embeddable_text=(base + line)[: self.policy.hard_truncate_chars] + "\n…[TRUNCATED]…",
                            content=table_md,
                        )
                    )
                    current = []
            else:
                current.append(line)

        if current:
            chunks.append(EmbeddableChunk(embeddable_text=base + "\n".join(current), content=table_md))

        return chunks

    def _get_table_caption(self, table: Any) -> str:
        """Best-effort extraction of a table caption as plain text."""
        caption = getattr(table, "caption", None)
        if caption is None:
            return ""

        text = getattr(caption, "text", None)
        if isinstance(text, str):
            return text
        if isinstance(caption, str):
            return caption
        return str(caption)

    def _format_table_prefix(self, *, caption: str, headers: list[str]) -> str:
        """Format the common prefix included in every table embedding chunk."""
        lines: list[str] = []
        if caption.strip():
            lines.append(f"TABLE: {caption.strip()}")
        if headers:
            lines.append("COLUMNS: " + " | ".join(headers))
        return "\n".join(lines).strip()

    def _display_excerpt(self, text: str, *, max_chars: int = 600) -> str:
        """A short, user-facing snippet when chunk.text is empty/missing."""
        s = (text or "").strip()
        if not s:
            return ""
        return s if len(s) <= max_chars else s[: max_chars - 1] + "…"
