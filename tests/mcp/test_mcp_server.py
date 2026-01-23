import time
from contextlib import asynccontextmanager
from multiprocessing import Process, set_start_method
from pathlib import Path

import httpx
import pytest
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.client.streamable_http import streamablehttp_client

from databao_context_engine.mcp.mcp_runner import run_mcp_server
from tests.mcp.conftest import ProjectWithRuns
from tests.utils.environment import env_variable

set_start_method("spawn")


@pytest.fixture
def anyio_backend(request):
    return "asyncio"


@asynccontextmanager
async def run_mcp_server_stdio_test(
    project_dir: Path,
    dce_path: Path,
    run_name: str | None = None,
):
    """Runs an MCP Server integration test by:
    1. Spawning a new process to run the MCP server in stdio mode
    2. Creating a client connecting with the MCP Server
    3. Yielding the MCP client session for the test to run
    """
    mcp_args = ["--run-name", run_name] if run_name else []
    mcp_args += ["--transport", "stdio"]
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
    run_name: str | None = None,
    host: str | None = None,
    port: int | None = None,
):
    """Runs a MCP Server integration test by:
    1. Spawning a new process to run the MCP server in streamable-http mode
    2. Creating a client connecting with the MCP Server (we're retrying 5 times to wait for the server to be up and running)
    3. Yielding the MCP client session for the test to run
    """
    server_process = Process(target=run_mcp_server, args=(project_dir, run_name, "streamable-http", host, port))
    with env_variable("DATABAO_CONTEXT_ENGINE_PATH", str(dce_path.resolve())):
        server_process.start()

    try:
        server_started = False
        attempts_left = 10
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
async def test_run_mcp_server__list_tools(dce_path: Path, project_with_runs: ProjectWithRuns):
    async with run_mcp_server_stdio_test(project_with_runs.project_dir, dce_path=dce_path) as session:
        # List available tools
        tools = await session.list_tools()
        assert len(tools.tools) == 2
        assert {tool.name for tool in tools.tools} == {"all_results_tool", "retrieve_tool"}


@pytest.mark.anyio
async def test_run_mcp_server__all_results_tool_with_no_run_name(dce_path: Path, project_with_runs: ProjectWithRuns):
    async with run_mcp_server_stdio_test(project_with_runs.project_dir, dce_path=dce_path) as session:
        all_results = await session.call_tool(name="all_results_tool", arguments={})
        run_contexts = next(
            iter(sorted(project_with_runs.runs, key=lambda run: run.run_name, reverse=True))
        ).datasource_contexts
        assert all(context.context in all_results.content[0].text for context in run_contexts)

        # Make sure the result only contains the contexts from the run
        all_contexts = {context for run in project_with_runs.runs for context in run.datasource_contexts}
        absent_context_in_run = all_contexts - set(run_contexts)
        assert all(context.context not in all_results.content[0].text for context in absent_context_in_run)


@pytest.mark.anyio
async def test_run_mcp_server__all_results_tool_with_run_name(dce_path: Path, project_with_runs: ProjectWithRuns):
    run_name = project_with_runs.runs[2].run_dir.name

    async with run_mcp_server_stdio_test(
        project_with_runs.project_dir, dce_path=dce_path, run_name=run_name
    ) as session:
        all_results = await session.call_tool(name="all_results_tool", arguments={})

        run_contexts = next(run for run in project_with_runs.runs if run.run_dir.name == run_name).datasource_contexts
        assert all(context.context in all_results.content[0].text for context in run_contexts)

        # Make sure the result only contains the contexts from the run
        all_contexts = {context for run in project_with_runs.runs for context in run.datasource_contexts}
        absent_context_in_run = all_contexts - set(run_contexts)
        assert all(context.context not in all_results.content[0].text for context in absent_context_in_run)


@pytest.mark.anyio
async def test_run_mcp_server__with_custom_host_and_port(dce_path: Path, project_with_runs: ProjectWithRuns):
    async with run_mcp_server_http_test(
        project_dir=project_with_runs.project_dir, dce_path=dce_path, host="localhost", port=8001
    ) as session:
        all_results = await session.call_tool(name="all_results_tool", arguments={})
        run_contexts = next(
            iter(sorted(project_with_runs.runs, key=lambda run: run.run_name, reverse=True))
        ).datasource_contexts
        assert all(context.context in all_results.content[0].text for context in run_contexts)

        # Make sure the result only contains the contexts from the run
        all_contexts = {context for run in project_with_runs.runs for context in run.datasource_contexts}
        absent_context_in_run = all_contexts - set(run_contexts)
        assert all(context.context not in all_results.content[0].text for context in absent_context_in_run)
