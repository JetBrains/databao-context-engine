import asyncio
from typing import Any

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer  # type: ignore

from databao_context_engine import DatabaoContextEngine, DatabaoContextPluginLoader, DatasourceId
from databao_context_engine.pluginlib.build_plugin import BuildPlugin, DatasourceType
from databao_context_engine.plugins.databases.postgresql.postgresql_db_plugin import (
    PostgresqlDbPlugin,
)
from tests.utils.project_creation import given_datasource_config_file


@pytest.fixture(scope="module")
def postgres_container():
    container = PostgresContainer("postgres:18.0", driver=None)
    container.start()
    try:
        yield container
    finally:
        container.stop()


def _get_connect_kwargs(postgres_container: PostgresContainer) -> dict[str, Any]:
    return {
        "host": postgres_container.get_container_host_ip(),
        "port": int(postgres_container.get_exposed_port(postgres_container.port)),
        "database": postgres_container.dbname,
        "user": postgres_container.username,
        "password": postgres_container.password,
    }


def _execute(postgres_container: PostgresContainer, sql: str) -> None:
    async def _run():
        conn = await asyncpg.connect(**_get_connect_kwargs(postgres_container))
        try:
            await conn.execute(sql)
        finally:
            await conn.close()

    asyncio.run(_run())


@pytest.fixture()
def pg_table(postgres_container: PostgresContainer):
    schema = "public"
    table = "run_sql_demo"
    _execute(
        postgres_container,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        );
        DELETE FROM {schema}.{table};
        INSERT INTO {schema}.{table} (name) VALUES ('alice'), ('bob');
        """,
    )
    yield (schema, table)
    _execute(postgres_container, f"DELETE FROM {schema}.{table};")


@pytest.fixture()
def engine_with_pg(project_path, postgres_container: PostgresContainer) -> DatabaoContextEngine:
    config = {
        "name": "pg_demo",
        "type": "postgres",
        "connection": {
            "host": postgres_container.get_container_host_ip(),
            "port": int(postgres_container.get_exposed_port(postgres_container.port)),
            "database": postgres_container.dbname,
            "user": postgres_container.username,
            "password": postgres_container.password,
        },
    }

    given_datasource_config_file(
        project_layout=DatabaoContextEngine(domain_dir=project_path)._project_layout,
        datasource_name="databases/pg_demo",
        config_content=config,
        overwrite_existing=True,
    )

    pg_plugin = PostgresqlDbPlugin()
    mapping: dict[DatasourceType, BuildPlugin] = {
        DatasourceType(full_type=t): pg_plugin for t in pg_plugin.supported_types()
    }
    return DatabaoContextEngine(domain_dir=project_path, plugin_loader=DatabaoContextPluginLoader(mapping))


def test_run_sql_postgresql_read_only_query(engine_with_pg: DatabaoContextEngine, pg_table):
    ds_id = DatasourceId.from_string_repr("databases/pg_demo.yaml")
    schema, table = pg_table

    res = engine_with_pg.run_sql(
        ds_id,
        f"SELECT id, name FROM {schema}.{table} ORDER BY id ASC",
        read_only=True,
    )

    assert res.columns == ["id", "name"]
    assert res.rows == [(1, "alice"), (2, "bob")]


def test_run_sql_postgresql_write_blocked_by_read_only(engine_with_pg: DatabaoContextEngine, pg_table):
    ds_id = DatasourceId.from_string_repr("databases/pg_demo.yaml")
    schema, table = pg_table

    with pytest.raises(PermissionError):
        engine_with_pg.run_sql(
            ds_id,
            f"INSERT INTO {schema}.{table} (name) VALUES ('charlie')",
            read_only=True,
        )
