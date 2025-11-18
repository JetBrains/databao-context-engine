from pathlib import Path
import yaml
import pytest

import nemory.build_sources.internal.plugin_execution as px
from nemory.build_sources.internal.source_discovery import SourceDescriptor
from nemory.pluginlib.build_plugin import BuildExecutionResult, BuildDatasourcePlugin, BuildFilePlugin, EmbeddableChunk
from datetime import datetime


def test_execute_yaml_uses_build_datasource_plugin(tmp_path: Path):
    cfg = _write(tmp_path / "src/files/cfg.yaml", yaml.safe_dump({"k": 1}))
    src = SourceDescriptor(path=cfg, main_type="files", subtype="yaml")
    res = px.execute(src, DSPlugin())
    assert res.type == "files/yaml"


def test_execute_raw_file_uses_build_file_plugin(tmp_path: Path):
    md = _writeb(tmp_path / "src/files/readme.md", b"hello world")
    src = SourceDescriptor(path=md, main_type="files", subtype="md")
    res = px.execute(src, FilePlugin())
    assert res.type == "files/md"


def test_execute_yaml_with_file_plugin_raises(tmp_path: Path):
    cfg = _write(tmp_path / "src/files/cfg.yaml", "k: 1\n")
    src = SourceDescriptor(path=cfg, main_type="files", subtype="yaml")
    with pytest.raises(TypeError):
        px.execute(src, FilePlugin())


def test_execute_raw_with_datasource_plugin_raises(tmp_path: Path):
    md = _writeb(tmp_path / "src/files/readme.md", b"hello")
    src = SourceDescriptor(path=md, main_type="files", subtype="yaml")
    with pytest.raises(TypeError):
        px.execute(src, DSPlugin())


def test_divide_into_chunks_forwards_call():
    res = BuildExecutionResult(
        id="x", name="n", type="files/md", description=None, version=None, executed_at=datetime.now(), result={}
    )
    chunks = px.divide_into_chunks(FilePlugin(), res)
    assert [c.embeddable_text for c in chunks] == ["t2"]


class DSPlugin(BuildDatasourcePlugin):
    name = "ds"

    def supported_types(self):
        return {"files/yaml"}

    def execute(self, *, full_type, datasource_name, file_config):
        assert full_type == "files/yaml"
        assert datasource_name == "cfg.yaml"
        assert file_config == {"k": 1}
        return BuildExecutionResult(
            id="id",
            name="nm",
            type=full_type,
            description=None,
            version=None,
            executed_at=datetime.now(),
            result={"ok": True},
        )

    def divide_result_into_chunks(self, build_result):
        return [EmbeddableChunk("t", "c")]


class FilePlugin(BuildFilePlugin):
    name = "file"

    def supported_types(self):
        return {"files/md"}

    def execute(self, *, full_type, file_name, file_buffer):
        assert full_type == "files/md"
        assert file_name == "readme.md"
        assert file_buffer.read(5) == b"hello"
        return BuildExecutionResult(
            id="id",
            name="nm",
            type=full_type,
            description=None,
            version=None,
            executed_at=datetime.now(),
            result={"ok": True},
        )

    def divide_result_into_chunks(self, build_result):
        return [EmbeddableChunk("t2", "c2")]


def _write(p: Path, text: str):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")
    return p


def _writeb(p: Path, data: bytes):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)
    return p
