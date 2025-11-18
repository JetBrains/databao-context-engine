import time
from contextlib import asynccontextmanager
from multiprocessing import Process

import httpx
import pytest
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from nemory.mcp.mcp_server import run_mcp_server
from tests.mcp.conftest import ProjectWithRuns


@pytest.fixture
def anyio_backend(request):
    return "asyncio"


@asynccontextmanager
async def run_mcp_server_test(
    project_dir: str, run_name: str | None = None, host: str | None = None, port: int | None = None
):
    """
    Runs a MCP Server integration test by:
    1. Spawning a new process to run the MCP server
    2. Creating a client connecting with the MCP Server (we're retrying 5 times to wait for the server to be up and running)
    3. Yielding the MCP client session for the test to run
    """
    server_process = Process(target=run_mcp_server, args=(project_dir, run_name, host, port))
    server_process.start()

    try:
        server_started = False
        attempts_left = 5
        while not server_started:
            try:
                async with streamablehttp_client(f"http://{host or '127.0.0.1'}:{port or 8000}/mcp") as (
                    read_stream,
                    write_stream,
                    _,
                ):
                    # Create a session using the client streams
                    async with ClientSession(read_stream, write_stream) as session:
                        # Initialize the connection
                        await session.initialize()

                        server_started = True

                        yield session
            except Exception as e:
                if _is_connection_error(e):
                    attempts_left -= 1
                    if attempts_left == 0:
                        raise AssertionError("Failed to connect to the MCP Server") from e
                    time.sleep(0.2)
                    continue

                # Don't ignore other failures (most importantly assertion failures)
                raise e
    finally:
        server_process.kill()


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
async def test_run_mcp_server__all_results_tool_with_no_run_name(project_with_runs: ProjectWithRuns):
    async with run_mcp_server_test(str(project_with_runs.project_dir)) as session:
        # List available tools
        tools = await session.list_tools()
        assert len(tools.tools) == 1
        assert tools.tools[0].name == "all_results_tool"

        all_results = await session.call_tool(name="all_results_tool", arguments={})
        assert (
            all_results.content[0].text
            == next(
                iter(sorted(project_with_runs.runs, key=lambda run: run.run_build_time, reverse=True))
            ).all_results_file_content
        )


@pytest.mark.anyio
async def test_run_mcp_server__all_results_tool_with_run_name(project_with_runs: ProjectWithRuns):
    run_name = project_with_runs.runs[2].run_dir.name

    async with run_mcp_server_test(str(project_with_runs.project_dir), run_name) as session:
        # List available tools
        tools = await session.list_tools()
        assert len(tools.tools) == 1
        assert tools.tools[0].name == "all_results_tool"

        all_results = await session.call_tool(name="all_results_tool", arguments={})
        assert (
            all_results.content[0].text
            == next(run for run in project_with_runs.runs if run.run_dir.name == run_name).all_results_file_content
        )


@pytest.mark.anyio
async def test_run_mcp_server__with_custom_host_and_port(project_with_runs: ProjectWithRuns):
    async with run_mcp_server_test(
        project_dir=str(project_with_runs.project_dir), host="localhost", port=8001
    ) as session:
        # List available tools
        tools = await session.list_tools()
        assert len(tools.tools) == 1
        assert tools.tools[0].name == "all_results_tool"

        all_results = await session.call_tool(name="all_results_tool", arguments={})
        assert (
            all_results.content[0].text
            == next(
                iter(sorted(project_with_runs.runs, key=lambda run: run.run_build_time, reverse=True))
            ).all_results_file_content
        )
