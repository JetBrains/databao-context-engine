from __future__ import annotations

from dataclasses import dataclass

from docling_core.types.doc.labels import DocItemLabel

from databao_context_engine.services.docling_chunker import DoclingChunker, EmbeddingPolicy, TokenSplitter


class FakeHfTokenizer:
    def __init__(self):
        self.model_max_length = 8192
        self._last_tokens: list[str] = []

    def encode(self, text: str, add_special_tokens: bool = False) -> list[int]:
        self._last_tokens = text.split()
        return list(range(len(self._last_tokens)))

    def decode(self, ids: list[int]) -> str:
        return " ".join(self._last_tokens[i] for i in ids)


class FakeDoclingTokenizer:
    def __init__(self):
        self._hf = FakeHfTokenizer()

    def count_tokens(self, text: str) -> int:
        return len(text.split())

    def get_tokenizer(self) -> FakeHfTokenizer:
        return self._hf


@dataclass
class FakeDocItem:
    label: object | None = None
    self_ref: str | None = None


@dataclass
class FakeMeta:
    doc_items: list[FakeDocItem]


@dataclass
class FakeChunk:
    text: str
    meta: FakeMeta


class FakeChunker:
    def __init__(self, chunks: list[FakeChunk]):
        self._chunks = chunks

    def chunk(self, dl_doc):
        yield from self._chunks

    def contextualize(self, chunk: FakeChunk) -> str:
        return chunk.text


class FakeValues:
    def __init__(self, rows: list[list[str]]):
        self._rows = rows

    def tolist(self) -> list[list[str]]:
        return self._rows


class FakeDataFrame:
    def __init__(self, columns: list[str], rows: list[list[str]]):
        self.columns = columns
        self._rows = rows
        self.values = FakeValues(rows)

    def astype(self, _type):
        return self


class FakeTable:
    def __init__(self, markdown: str, df: FakeDataFrame | None, caption: str = ""):
        self._markdown = markdown
        self._df = df
        self.caption = caption

    def export_to_markdown(self, doc):
        return self._markdown

    def export_to_dataframe(self, doc):
        if self._df is None:
            raise AssertionError("export_to_dataframe() should NOT be called in this test.")
        return self._df


def test_token_splitter_splits_with_overlap():
    policy = EmbeddingPolicy(tokens_budget=5, tokens_overlap=2)
    splitter = TokenSplitter(policy)
    tokenizer = FakeDoclingTokenizer()

    text = "t0 t1 t2 t3 t4 t5 t6 t7 t8 t9 t10 t11"
    parts = splitter.split(text, tokenizer=tokenizer)

    assert parts == [
        "t0 t1 t2 t3 t4",
        "t3 t4 t5 t6 t7",
        "t6 t7 t8 t9 t10",
        "t9 t10 t11",
    ]


def test_token_splitter_returns_single_part_if_under_budget():
    policy = EmbeddingPolicy(tokens_budget=10, tokens_overlap=2)
    splitter = TokenSplitter(policy)
    tokenizer = FakeDoclingTokenizer()

    text = "a b c"
    assert splitter.split(text, tokenizer=tokenizer) == ["a b c"]


def test_is_table_chunk_detects_by_label():
    idx = DoclingChunker()

    chunk = FakeChunk(
        text="doesn't matter",
        meta=FakeMeta(doc_items=[FakeDocItem(label=DocItemLabel.TABLE)]),
    )
    assert idx._is_table_chunk(chunk) is True


def test_is_table_chunk_detects_by_self_ref():
    idx = DoclingChunker()

    chunk = FakeChunk(
        text="doesn't matter",
        meta=FakeMeta(doc_items=[FakeDocItem(self_ref="#/tables/0")]),
    )
    assert idx._is_table_chunk(chunk) is True


def test_is_table_chunk_false_for_normal_text():
    idx = DoclingChunker()

    chunk = FakeChunk(
        text="hello",
        meta=FakeMeta(doc_items=[FakeDocItem(label="TEXT", self_ref="#/texts/1")]),
    )
    assert idx._is_table_chunk(chunk) is False


def test_table_fast_path_does_not_export_dataframe():
    policy = EmbeddingPolicy(tokens_budget=50, tokens_overlap=5)
    idx = DoclingChunker(policy=policy)
    tokenizer = FakeDoclingTokenizer()

    table = FakeTable(
        markdown="| a | b |\n|---|---|\n| 1 | 2 |",
        df=None,
        caption="Small table",
    )

    chunks = idx._table_to_chunks(table=table, doc=object(), tokenizer=tokenizer)
    assert len(chunks) == 1
    assert "TABLE: Small table" in chunks[0].embeddable_text
    assert "| a | b |" in chunks[0].content


def test_table_slow_path_row_batches_without_losing_rows():
    policy = EmbeddingPolicy(tokens_budget=20, tokens_overlap=5)
    idx = DoclingChunker(policy=policy)
    tokenizer = FakeDoclingTokenizer()

    big_markdown = "x " * 200

    columns = ["id", "notes"]
    rows = [[str(i), f"row{i}"] for i in range(1, 11)]
    df = FakeDataFrame(columns=columns, rows=rows)

    table = FakeTable(markdown=big_markdown, df=df, caption="Big table")

    chunks = idx._table_to_chunks(table=table, doc=object(), tokenizer=tokenizer)
    assert len(chunks) >= 2

    combined = "\n".join(c.embeddable_text for c in chunks)
    for i in range(1, 11):
        assert f"- {i} | row{i}" in combined


def test_index_skips_table_chunks_from_text_and_indexes_tables_separately():
    policy = EmbeddingPolicy(tokens_budget=50, tokens_overlap=5)
    idx = DoclingChunker(policy=policy)

    tokenizer = FakeDoclingTokenizer()

    normal_chunk = FakeChunk(
        text="normal text chunk",
        meta=FakeMeta(doc_items=[FakeDocItem(label="TEXT", self_ref="#/texts/1")]),
    )
    table_chunk = FakeChunk(
        text="table text that should be skipped",
        meta=FakeMeta(doc_items=[FakeDocItem(label=DocItemLabel.TABLE, self_ref="#/tables/0")]),
    )
    chunker = FakeChunker([normal_chunk, table_chunk])

    df = FakeDataFrame(columns=["id"], rows=[["1"], ["2"], ["3"]])
    table = FakeTable(markdown="x " * 200, df=df, caption="Big table")

    class FakeDoc:
        tables = [table]

    idx._make_tokenizer = lambda: tokenizer
    idx._make_chunker = lambda _tok: chunker

    results = idx.index(FakeDoc())

    texts = [c.embeddable_text for c in results]

    assert any("normal text chunk" in t for t in texts)
    assert not any("table text that should be skipped" in t for t in texts)
    assert any("COLUMNS:" in t or "ROWS:" in t for t in texts)
