from datetime import datetime
from pathlib import Path

import yaml

from databao_context_engine.build_sources.export_results import (
    export_build_result,
)
from databao_context_engine.build_sources.plugin_execution import BuiltDatasourceContext


def _output_dir(tmp_path: Path) -> Path:
    return tmp_path.joinpath("output")


def _make_result(*, id: str, full_type: str, payload: object) -> BuiltDatasourceContext:
    return BuiltDatasourceContext(
        datasource_id=id,
        datasource_type=full_type,
        context_built_at=datetime.now(),
        context=payload,
    )


def test_export_build_result_writes_yaml(tmp_path: Path) -> None:
    output_dir = _output_dir(tmp_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    res = _make_result(
        id="databases/Datasource 1.yaml",
        full_type="databases/my-db",
        payload={"tables": [{"name": "t1"}]},
    )

    export_build_result(output_dir, res)

    out = output_dir / "databases" / "Datasource 1.yaml"
    assert out.is_file()
    data = yaml.safe_load(out.read_text())
    assert data["datasource_id"] == "databases/Datasource 1.yaml"
    assert data["datasource_type"] == "databases/my-db"
    assert data["context"] == res.context
