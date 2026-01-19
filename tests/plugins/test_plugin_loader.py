import pytest

from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.plugins.plugin_loader import (
    DuplicatePluginTypeError,
    merge_plugins,
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
    reg = merge_plugins([P1()], [P2()])
    assert set(reg.keys()) == {
        DatasourceType(full_type="files/md"),
        DatasourceType(full_type="databases/pg"),
        DatasourceType(full_type="files/txt"),
    }
    assert reg[DatasourceType(full_type="files/md")].name == "p1"
    assert reg[DatasourceType(full_type="files/txt")].name == "p2"


def test_merge_plugins_duplicate_raises():
    with pytest.raises(DuplicatePluginTypeError) as e:
        merge_plugins([P1()], [P3Overlap()])
    msg = str(e.value)
    assert "files/md" in msg
    assert "P1" in msg or "p1" in msg
    assert "P3Overlap" in msg
