from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace


class FakeRunDTO:
    def __init__(self, run_id: int, started_at: datetime | None = None):
        self.run_id = run_id
        self.started_at = started_at or (datetime.now() - timedelta(seconds=1))


class FakeRunRepository:
    def __init__(self):
        self.created: list[tuple[str, str | None, FakeRunDTO]] = []
        self.updated: list[tuple[int, datetime | None, str | None]] = []
        self._next_id = 1

    def create(self, *, project_id: str, dce_version: str | None):
        dto = FakeRunDTO(self._next_id)
        self._next_id += 1
        self.created.append((project_id, dce_version, dto))
        return dto

    def update(self, run_id: int, *, ended_at: datetime | None = None, dce_version: str | None = None):
        self.updated.append((run_id, ended_at, dce_version))
        return FakeRunDTO(run_id)


class FakeDatasourceRunRepository:
    def __init__(self):
        self.created: list[tuple[int, str, str, str, object]] = []
        self._next_id = 1

    def create(self, *, run_id: int, plugin: str, source_id: str, storage_directory: str):
        dto = SimpleNamespace(datasource_run_id=self._next_id)
        self._next_id += 1
        self.created.append((run_id, plugin, source_id, storage_directory, dto))
        return dto


class FakeChunkEmbeddingService:
    def __init__(self):
        self.calls: list[tuple[int, list]] = []

    def embed_chunks(self, *, datasource_run_id: int, chunks):
        self.calls.append((datasource_run_id, list(chunks)))


class FakeSource:
    def __init__(self, path: Path, main_type: str, subtype: str):
        self.path = path
        self.main_type = main_type
        self.subtype = subtype
