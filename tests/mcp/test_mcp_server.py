import asyncio
import time
from contextlib import asynccontextmanager
from multiprocessing import Process, set_start_method
from pathlib import Path

import httpx
import pytest
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.client.streamable_http import streamable_http_client

from databao_context_engine.mcp.mcp_runner import run_mcp_server
from tests.mcp.conftest import Project
from tests.utils.environment import env_variable

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


@pytest.mark.anyio
async def test_run_mcp_server__list_tools(dce_path: Path, project: Project):
    async with run_mcp_server_stdio_test(project.project_dir, dce_path=dce_path) as session:
        # List available tools
        tools = await session.list_tools()
        assert len(tools.tools) == 4
        assert {tool.name for tool in tools.tools} == {
            "all_results_tool",
            "retrieve_tool",
            "run_sql_tool",
            "list_datasources_tool",
        }


@pytest.mark.anyio
async def test_run_mcp_server__all_results_tool(dce_path: Path, project: Project):
    async with run_mcp_server_stdio_test(project.project_dir, dce_path=dce_path) as session:
        all_results = await session.call_tool(name="all_results_tool", arguments={})

        assert all(context.context in all_results.content[0].text for context in project.output.datasource_contexts)


@pytest.mark.anyio
async def test_run_mcp_server__with_custom_host_and_port(dce_path: Path, project: Project):
    async with run_mcp_server_http_test(
        project_dir=project.project_dir, dce_path=dce_path, host="localhost", port=8001
    ) as session:
        all_results = await session.call_tool(name="all_results_tool", arguments={})
        assert all(context.context in all_results.content[0].text for context in project.output.datasource_contexts)
