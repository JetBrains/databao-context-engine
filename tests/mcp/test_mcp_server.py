import asyncio
import json
import time
import uuid
from contextlib import asynccontextmanager
from multiprocessing import Process, set_start_method
from pathlib import Path

import httpx
import pytest
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.client.streamable_http import streamable_http_client

from databao_context_engine.build_sources.plugin_execution import BuiltDatasourceContext
from databao_context_engine.datasources.types import DatasourceId
from databao_context_engine.llm.config import EmbeddingModelDetails
from databao_context_engine.mcp.mcp_runner import run_mcp_server
from databao_context_engine.plugins.databases.databases_types import (
    DatabaseCatalog,
    DatabaseColumn,
    DatabaseIntrospectionResult,
    DatabaseSchema,
    DatabaseTable,
)
from databao_context_engine.project.layout import ProjectLayout
from databao_context_engine.project.project_config import ProjectConfig
from databao_context_engine.serialization.yaml import to_yaml_string
from tests.mcp.conftest import Project
from tests.utils.environment import env_variable
from tests.utils.project_creation import given_datasource_config_file, given_output_dir_with_built_contexts

set_start_method("spawn")


@pytest.fixture
def anyio_backend(request):
    return "asyncio"


async def _wait_for_port(host: str, port: int, timeout: float = 30.0):
    start = time.monotonic()
    while True:
        try:
            reader, writer = await asyncio.open_connection(host, port)
            writer.close()
            await writer.wait_closed()
            return
        except OSError as e:
            if time.monotonic() - start > timeout:
                raise TimeoutError(f"Server did not open {host}:{port}") from e
            await asyncio.sleep(0.1)


@asynccontextmanager
async def run_mcp_server_stdio_test(
    project_dir: Path,
    dce_path: Path,
):
    """Runs an MCP Server integration test by:
    1. Spawning a new process to run the MCP server in stdio mode
    2. Creating a client connecting with the MCP Server
    3. Yielding the MCP client session for the test to run
    """
    mcp_args = ["--transport", "stdio"]
    async with stdio_client(
        StdioServerParameters(
            command="uv",
            args=["run", "dce", "--project-dir", str(project_dir.resolve()), "mcp"] + mcp_args,
            env={"DATABAO_CONTEXT_ENGINE_PATH": str(dce_path.resolve())},
        )
    ) as (
        read,
        write,
    ):
        async with ClientSession(read, write) as session:
            await session.initialize()
            yield session


@asynccontextmanager
async def run_mcp_server_http_test(
    project_dir: Path,
    dce_path: Path,
    host: str | None = None,
    port: int | None = None,
):
    """Runs a MCP Server integration test by:
    1. Spawning a new process to run the MCP server in streamable-http mode
    2. Waiting until the server is ready and listening on the specified host and port
    3. Creating a client connected to the MCP Server
    4. Yielding the MCP client session for the test to run
    """
    host = host or "127.0.0.1"
    port = port or 8000

    server_process = Process(
        target=run_mcp_server,
        args=(project_dir, "streamable-http", host, port),
    )

    with env_variable("DATABAO_CONTEXT_ENGINE_PATH", str(dce_path.resolve())):
        server_process.start()

    try:
        await _wait_for_port(host, port)

        async with streamable_http_client(f"http://{host}:{port}/mcp") as (
            read_stream,
            write_stream,
            _,
        ):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                yield session

    finally:
        if server_process.is_alive():
            server_process.kill()
        server_process.join()


def _is_connection_error(e: Exception) -> bool:
    if isinstance(e, httpx.ConnectError):
        return True

    if isinstance(e, ExceptionGroup):
        return (
            next(
                (
                    actual_exception
                    for actual_exception in e.exceptions
                    if isinstance(actual_exception, httpx.ConnectError)
                ),
                None,
            )
            is not None
        )

    return False


def _database_context_yaml(datasource_id: str, datasource_type: str = "postgres") -> str:
    return to_yaml_string(
        BuiltDatasourceContext(
            datasource_id=datasource_id,
            datasource_type=datasource_type,
            context=DatabaseIntrospectionResult(
                catalogs=[
                    DatabaseCatalog(
                        name="analytics",
                        schemas=[
                            DatabaseSchema(
                                name="public",
                                tables=[
                                    DatabaseTable(
                                        name="orders",
                                        description="Customer orders",
                                        columns=[DatabaseColumn(name="id", type="INTEGER", nullable=False)],
                                        samples=[],
                                    ),
                                    DatabaseTable(
                                        name="products",
                                        description="Customer products",
                                        columns=[DatabaseColumn(name="id", type="INTEGER", nullable=False)],
                                        samples=[],
                                    ),
                                ],
                            )
                        ],
                    )
                ]
            ),
        )
    )


@pytest.mark.anyio
async def test_run_mcp_server__list_tools(dce_path: Path, project: Project):
    async with run_mcp_server_stdio_test(project.project_dir, dce_path=dce_path) as session:
        tools = await session.list_tools()
        assert len(tools.tools) == 6
        assert {tool.name for tool in tools.tools} == {
            "search_context",
            "run_sql_on_database",
            "list_all_datasources",
            "list_database_datasources",
            "list_database_schemas",
            "get_database_table_details",
        }


@pytest.mark.anyio
async def test_run_mcp_server__all_results_tool(dce_path: Path, project: Project):
    async with run_mcp_server_stdio_test(project.project_dir, dce_path=dce_path) as session:
        all_datasources = await session.call_tool(name="list_all_datasources", arguments={})

        assert json.loads(all_datasources.content[0].text) == {
            "datasources": [
                {"id": "dummy/my_datasource.yaml", "name": "my_datasource", "type": "dummy"},
                {"id": "main_type/datasource_name.yaml", "name": "datasource_name", "type": "postgres"},
            ]
        }


@pytest.mark.anyio
async def test_run_mcp_server__with_custom_host_and_port(dce_path: Path, project: Project):
    async with run_mcp_server_http_test(
        project_dir=project.project_dir, dce_path=dce_path, host="localhost", port=8001
    ) as session:
        all_datasources = await session.call_tool(name="list_all_datasources", arguments={})
        assert json.loads(all_datasources.content[0].text) == {
            "datasources": [
                {"id": "dummy/my_datasource.yaml", "name": "my_datasource", "type": "dummy"},
                {"id": "main_type/datasource_name.yaml", "name": "datasource_name", "type": "postgres"},
            ]
        }


@pytest.mark.anyio
async def test_run_mcp_server__database_metadata_tools(dce_path: Path, project: Project):
    project_dir = project.project_dir

    project_layout = ProjectLayout(
        project_dir=project_dir,
        project_config=ProjectConfig(
            project_id=uuid.uuid4(),
            ollama_embedding_model_details=EmbeddingModelDetails.default(),
        ),
    )

    given_datasource_config_file(
        project_layout=project_layout,
        datasource_name="databases/warehouse",
        config_content={"type": "postgres", "name": "warehouse"},
    )
    given_datasource_config_file(
        project_layout=project_layout,
        datasource_name="analytics/dbt_project",
        config_content={"type": "dbt", "name": "dbt_project", "project-dir": "/tmp/dbt"},
    )
    given_output_dir_with_built_contexts(
        project_layout,
        [
            (
                DatasourceId.from_string_repr("databases/warehouse.yaml"),
                _database_context_yaml("databases/warehouse.yaml"),
            ),
        ],
    )

    async with run_mcp_server_stdio_test(project.project_dir, dce_path=dce_path) as session:
        datasources = await session.call_tool(name="list_database_datasources", arguments={})
        assert "databases/warehouse.yaml" in datasources.content[0].text
        assert "analytics/dbt_project.yaml" not in datasources.content[0].text

        schema_tree = await session.call_tool(
            name="list_database_schemas",
            arguments={"datasource_id": "databases/warehouse.yaml"},
        )
        schema_list_json = json.loads(schema_tree.content[0].text)
        assert schema_list_json["schemas"][0]["catalog_name"] == "analytics"
        assert schema_list_json["schemas"][0]["schema_name"] == "public"
        assert schema_list_json["schemas"][0]["tables"][0]["table_name"] == "orders"

        table_details = await session.call_tool(
            name="get_database_table_details",
            arguments={
                "datasource_id": "databases/warehouse.yaml",
                "catalog": "analytics",
                "schema": "public",
                "table": "orders",
            },
        )

        table_details_json = json.loads(table_details.content[0].text)
        assert table_details_json["datasource_id"] == "databases/warehouse.yaml"
        assert table_details_json["table"]["description"] == "Customer orders"
        assert "unique_constraints" not in table_details_json["table"]
        assert "stats" not in table_details_json["table"]
