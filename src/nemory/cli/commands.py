from importlib.metadata import version
from pathlib import Path

import click
from click import Context

from nemory.build_sources.public.api import build_all_datasources
from nemory.config.logging import configure_logging
from nemory.mcp.mcp_server import McpTransport, run_mcp_server
from nemory.project.init_project import init_project_dir
from nemory.project.layout import read_config_file
from nemory.query_embeddings.internal.query_wiring import query_embeddings
from nemory.storage.migrate import migrate


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging")
@click.option(
    "-d",
    "--project-dir",
    type=click.STRING,
    help="Location of your Nemory project",
)
@click.pass_context
def nemory(ctx: Context, verbose: bool, project_dir: str | None) -> None:
    if project_dir is None:
        project_dir = str(Path.cwd())

    configure_logging(verbose=verbose, project_dir=project_dir)

    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["project_dir"] = project_dir

    migrate()


@nemory.command()
@click.pass_context
def info(ctx: Context) -> None:
    """
    Display system-wide information
    """

    project_dir = ctx.obj["project_dir"]

    click.echo(f"Nemory version: {version('nemory')}")
    click.echo(f"Is verbose? {ctx.obj['verbose']}")
    click.echo(f"Project dir: {project_dir}")
    click.echo(f"Project ID: {read_config_file(Path(project_dir)).project_id}")


@nemory.command()
@click.pass_context
def init(ctx: Context) -> None:
    """
    Create an empty Nemory project
    """
    init_project_dir(project_dir=ctx.obj["project_dir"])


@nemory.command()
@click.pass_context
def build(ctx: Context) -> None:
    build_all_datasources(project_dir=ctx.obj["project_dir"])


@nemory.command()
@click.argument(
    "query-text",
    nargs=-1,
    required=True,
)
@click.option(
    "-r",
    "--run-name",
    type=click.STRING,
    help="Build run to use (the run folder name). Defaults to the latest run in the project.",
)
@click.option(
    "-l",
    "--limit",
    type=click.INT,
    help="Maximum number of chunk matches to return.",
)
@click.pass_context
def query(ctx: Context, query_text: tuple[str, ...], run_name: str | None, limit: int | None) -> None:
    """
    Search the project's built context for the most relevant chunks.
    """
    text = " ".join(query_text)
    query_embeddings(project_dir=ctx.obj["project_dir"], query_text=text, run_name=run_name, limit=limit or 50)


@nemory.command()
@click.option(
    "-r",
    "--run-name",
    type=click.STRING,
    help="Name of the build run you want to use (aka. the name of the run folder in your project's output). Defaults to the latest one in the project.",
)
@click.option(
    "-H",
    "--host",
    type=click.STRING,
    help="Host to bind to. Defaults to 127.0.0.1",
)
@click.option(
    "-p",
    "--port",
    type=click.INT,
    help="Port to bind to. Defaults to 8000",
)
@click.option(
    "-t",
    "--transport",
    type=click.Choice(["stdio", "streamable-http"]),
    default="stdio",
    help="Transport to use. Defaults to stdio",
)
@click.pass_context
def mcp(ctx: Context, run_name: str | None, host: str | None, port: int | None, transport: McpTransport) -> None:
    """
    Run Nemory's MCP server
    """
    run_mcp_server(project_dir=ctx.obj["project_dir"], run_name=run_name, transport=transport, host=host, port=port)
