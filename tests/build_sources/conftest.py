from pathlib import Path

import pytest

from nemory.build_sources.internal import build_runner
from tests.utils.fakes import FakeSource


@pytest.fixture
def stub_plugins(mocker):
    def _stub(mapping):
        return mocker.patch.object(build_runner, "load_plugins", return_value=mapping)

    return _stub


@pytest.fixture
def stub_sources(mocker):
    def _stub(sources):
        return mocker.patch.object(build_runner, "discover_datasources", return_value=sources)

    return _stub


@pytest.fixture
def fake_run_dir(mocker, tmp_path: Path):
    path = tmp_path / "out" / "run-1"
    mocker.patch.object(build_runner, "create_run_dir", return_value=path)
    return path


@pytest.fixture
def chunk_embedding_service(mocker):
    svc = mocker.Mock(name="ChunkEmbeddingService")
    svc.embed_chunks = mocker.Mock()
    return svc


@pytest.fixture
def FakeSourceClass():
    return FakeSource
