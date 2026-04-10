from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import asyncpg
import pytest
import yaml
from testcontainers.postgres import PostgresContainer  # type: ignore

from tests.integration.sqlite_integration_test_utils import create_sqlite_with_base_schema

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "build_dbt_database_context.py"


@pytest.fixture(scope="module")
def postgres_container() -> Iterator[PostgresContainer]:
    container = PostgresContainer("postgres:18.0", driver=None)
    container.start()
    try:
        yield container
    finally:
        container.stop()


def _get_postgres_connect_kwargs(postgres_container: PostgresContainer) -> dict[str, Any]:
    return {
        "host": postgres_container.get_container_host_ip(),
        "port": int(postgres_container.get_exposed_port(postgres_container.port)),
        "database": postgres_container.dbname,
        "user": postgres_container.username,
        "password": postgres_container.password,
    }


def _execute_postgres(postgres_container: PostgresContainer, sql: str) -> None:
    async def _run() -> None:
        conn = await asyncpg.connect(**_get_postgres_connect_kwargs(postgres_container))
        try:
            await conn.execute(sql)
        finally:
            await conn.close()

    asyncio.run(_run())


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


def test_build_dbt_database_context__postgres_end_to_end(tmp_path: Path, postgres_container: PostgresContainer) -> None:
    table_name = "dbt_context_postgres_demo"
    _execute_postgres(
        postgres_container,
        f"""
        DROP TABLE IF EXISTS public.{table_name};
        CREATE TABLE public.{table_name} (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL
        );
        INSERT INTO public.{table_name} (id, email) VALUES (1, 'alice@example.com');
        """,
    )

    project_path = tmp_path / "dbt_postgres_project"
    project_path.mkdir()
    (project_path / "models").mkdir()
    (project_path / "models" / "example.sql").write_text("select 1 as id\n", encoding="utf-8")
    (project_path / "dbt_project.yml").write_text(
        "\n".join(
            [
                "name: postgres_demo",
                "version: 1.0.0",
                "config-version: 2",
                "profile: postgres_demo_profile",
                'model-paths: ["models"]',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (project_path / "profiles.yml").write_text(
        "\n".join(
            [
                "postgres_demo_profile:",
                "  target: dev",
                "  outputs:",
                "    dev:",
                "      type: postgres",
                f"      host: {postgres_container.get_container_host_ip()}",
                f"      port: {int(postgres_container.get_exposed_port(postgres_container.port))}",
                f"      dbname: {postgres_container.dbname}",
                f"      user: {postgres_container.username}",
                f"      password: {postgres_container.password}",
                "      schema: public",
                "      threads: 1",
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

    try:
        assert result.returncode == 0, result.stderr

        document = yaml.safe_load(result.stdout)

        assert document["datasource_id"] == "dbt_postgres_project.yaml"
        assert document["datasource_type"] == "postgres"

        table_names = {
            table["name"]
            for catalog in document["context"]["catalogs"]
            for schema in catalog["schemas"]
            for table in schema["tables"]
        }
        assert table_name in table_names
    finally:
        _execute_postgres(postgres_container, f"DROP TABLE IF EXISTS public.{table_name};")
