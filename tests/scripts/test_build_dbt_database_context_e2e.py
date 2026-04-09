from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import yaml

from tests.integration.sqlite_integration_test_utils import create_sqlite_with_base_schema

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "build_dbt_database_context.py"


def test_build_dbt_database_context__sqlite_end_to_end(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "warehouse.sqlite"
    create_sqlite_with_base_schema(sqlite_path)

    project_path = tmp_path / "dbt_sqlite_project"
    project_path.mkdir()
    (project_path / "models").mkdir()
    (project_path / "models" / "example.sql").write_text("select 1 as id\n", encoding="utf-8")
    (project_path / "dbt_project.yml").write_text(
        "\n".join(
            [
                "name: sqlite_demo",
                "version: 1.0.0",
                "config-version: 2",
                "profile: sqlite_demo_profile",
                'model-paths: ["models"]',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (project_path / "profiles.yml").write_text(
        "\n".join(
            [
                "sqlite_demo_profile:",
                "  target: dev",
                "  outputs:",
                "    dev:",
                "      type: sqlite",
                "      threads: 1",
                "      database: warehouse",
                "      schema: main",
                "      schemas_and_paths:",
                f"        main: {sqlite_path}",
                f"      schema_directory: {tmp_path}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    env = {**os.environ, "DBT_PROFILES_DIR": str(project_path)}
    result = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), str(project_path)],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0, result.stderr

    document = yaml.safe_load(result.stdout)

    assert document["datasource_id"] == "dbt_sqlite_project.yaml"
    assert document["datasource_type"] == "sqlite"

    catalogs = document["context"]["catalogs"]
    tables = catalogs[0]["schemas"][0]["tables"]
    table_names = {table["name"] for table in tables}
    assert "users" in table_names
