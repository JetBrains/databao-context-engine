import subprocess

import pytest

from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.plugins.plugin_loader import (
    DuplicatePluginTypeError,
    _merge_plugins,
)


class P1:
    name = "p1"

    def supported_types(self) -> set[str]:
        return {"files/md", "databases/pg"}


class P2:
    name = "p2"

    def supported_types(self) -> set[str]:
        return {"files/txt"}


class P3Overlap:
    name = "p3"

    def supported_types(self) -> set[str]:
        return {"files/md"}  # overlaps with P1


def test_merge_plugins():
    reg = _merge_plugins([P1()], [P2()])
    assert set(reg.keys()) == {
        DatasourceType(full_type="files/md"),
        DatasourceType(full_type="databases/pg"),
        DatasourceType(full_type="files/txt"),
    }
    assert reg[DatasourceType(full_type="files/md")].name == "p1"
    assert reg[DatasourceType(full_type="files/txt")].name == "p2"


def test_merge_plugins_duplicate_raises():
    with pytest.raises(DuplicatePluginTypeError) as e:
        _merge_plugins([P1()], [P3Overlap()])
    msg = str(e.value)
    assert "files/md" in msg
    assert "P1" in msg or "p1" in msg
    assert "P3Overlap" in msg


def test_loaded_plugins_no_extra():
    p = subprocess.Popen(
        ["uv", "run", "--isolated", "--extra", "recommended", "-s", "tests/plugins/get_loaded_plugins.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    lines = []
    for line in p.stdout.readlines():
        lines.append(line.decode())
    exit_code = p.wait()
    assert exit_code == 0, lines
    output = "\n".join(lines)
    plugin_ids = eval(output)
    assert plugin_ids == {
        "jetbrains/duckdb",
        "jetbrains/mysql",
        "jetbrains/postgres",
        "jetbrains/parquet",
        "jetbrains/unstructured_files",
    }
