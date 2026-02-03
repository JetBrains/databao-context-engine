from __future__ import annotations

from pathlib import Path

import pytest
from docling_core.transforms.chunker.tokenizer.huggingface import HuggingFaceTokenizer

from databao_context_engine.plugins.files.pdf_plugin import PDFPlugin
from databao_context_engine.services.docling_chunker import EmbeddingPolicy

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _embed_tokenizer(policy: EmbeddingPolicy) -> HuggingFaceTokenizer:
    return HuggingFaceTokenizer.from_pretrained(
        model_name=policy.model_name,
        max_tokens=policy.tokens_budget,
    )


def test_pdf_plugin_smoke_table_pdf_produces_chunks_under_budget():
    pdf_path = FIXTURES_DIR / "long_table.pdf"
    assert pdf_path.exists(), f"Missing fixture: {pdf_path}"

    plugin = PDFPlugin()

    with pdf_path.open("rb") as f:
        doc = plugin.build_file_context(full_type="files/pdf", file_name=pdf_path.name, file_buffer=f)

    chunks = plugin.divide_context_into_chunks(doc)

    assert chunks
    assert 1 < len(chunks) < 5000
    assert all(c.embeddable_text.strip() for c in chunks)
    assert all(c.content.strip() for c in chunks)

    unique_embed_texts = {c.embeddable_text for c in chunks}
    assert len(unique_embed_texts) >= int(len(chunks) * 0.8)

    policy = EmbeddingPolicy()
    tok = _embed_tokenizer(policy)
    over_budget = [c for c in chunks if tok.count_tokens(c.embeddable_text) > policy.tokens_budget]
    assert not over_budget

    table_chunks = [c for c in chunks if _is_table_chunk_text(c.embeddable_text)]
    non_table_chunks = [c for c in chunks if not _is_table_chunk_text(c.embeddable_text)]

    assert table_chunks
    assert len(table_chunks) >= 2

    assert len(non_table_chunks) <= 2

    assert any("\n- " in c.embeddable_text for c in table_chunks), (
        "Expected at least one '- ...' row line in table chunks"
    )
    assert any(" | " in c.embeddable_text for c in table_chunks)

    assert all("|" in c.content for c in table_chunks)


def test_pdf_plugin_smoke_mixed_text_pdf_produces_chunks_under_budget():
    pdf_path = FIXTURES_DIR / "mixed_text.pdf"
    if not pdf_path.exists():
        pytest.skip(f"Optional fixture not present: {pdf_path}")

    plugin = PDFPlugin()

    with pdf_path.open("rb") as f:
        doc = plugin.build_file_context(full_type="files/pdf", file_name=pdf_path.name, file_buffer=f)

    chunks = plugin.divide_context_into_chunks(doc)

    assert chunks
    assert 1 < len(chunks) < 5000
    assert all(c.embeddable_text.strip() for c in chunks)
    assert all(c.content.strip() for c in chunks)

    unique_embed_texts = {c.embeddable_text for c in chunks}
    assert len(unique_embed_texts) >= int(len(chunks) * 0.8)

    policy = EmbeddingPolicy()
    tok = _embed_tokenizer(policy)
    over_budget = [c for c in chunks if tok.count_tokens(c.embeddable_text) > policy.tokens_budget]
    assert not over_budget

    table_chunks = [c for c in chunks if _is_table_chunk_text(c.embeddable_text)]
    prose_chunks = [c for c in chunks if not _is_table_chunk_text(c.embeddable_text)]

    assert prose_chunks
    assert table_chunks

    assert all(not _is_table_chunk_text(c.embeddable_text) for c in prose_chunks)

    assert all(_is_table_chunk_text(c.embeddable_text) for c in table_chunks)

    assert any(c.embeddable_text.strip().startswith("TABLE:") for c in table_chunks) or any(
        ("COLUMNS:" in c.embeddable_text and "ROWS:" in c.embeddable_text) for c in table_chunks
    )

    row_batched = [c for c in table_chunks if ("COLUMNS:" in c.embeddable_text and "ROWS:" in c.embeddable_text)]
    if row_batched:
        assert any("\n- " in c.embeddable_text for c in row_batched), (
            "Expected at least one row line in row-batched table chunks"
        )
        assert any(" | " in c.embeddable_text for c in row_batched), (
            "Expected pipe-separated cells in row-batched table chunks"
        )

    assert all("|" in c.content for c in table_chunks)


def _is_table_chunk_text(s: str) -> bool:
    s = s.strip()
    return s.startswith("TABLE:") or (("COLUMNS:" in s) and ("ROWS:" in s))
