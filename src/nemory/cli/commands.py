import sys
from pathlib import Path

import click
from click import Context

from nemory.build_sources.public.api import build_all_datasources
from nemory.config.logging import configure_logging
from nemory.datasource_config.add_config import add_datasource_config as add_datasource_config_internal
from nemory.datasource_config.validate_config import validate_datasource_config as validate_datasource_config_internal
from nemory.llm.install import resolve_ollama_bin
from nemory.mcp.mcp_runner import McpTransport, run_mcp_server
from nemory.project.info import get_command_info
from nemory.project.init_project import InitErrorReason, InitProjectError, init_project_dir
from nemory.project.layout import create_project_dir
from nemory.retrieve_embeddings.internal.retrieve_wiring import retrieve_embeddings
from nemory.storage.migrate import migrate


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging")
@click.option("-q", "--quiet", is_flag=True, help="Disable all console logging")
@click.option(
    "-d",
    "--project-dir",
    type=click.STRING,
    help="Location of your Nemory project",
)
@click.pass_context
def nemory(ctx: Context, verbose: bool, quiet: bool, project_dir: str | None) -> None:
    if verbose and quiet:
        print("Arguments --quiet and --verbose can not be used together", file=sys.stderr)
        exit(1)

    if project_dir is None:
        project_path = Path.cwd()
    else:
        project_path = Path(project_dir).expanduser()

    configure_logging(verbose=verbose, quiet=quiet, project_dir=project_path)

    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["quiet"] = quiet
    ctx.obj["project_dir"] = project_path

    migrate()


@nemory.command()
@click.pass_context
def info(ctx: Context) -> None:
    """
    Display system-wide information
    """

    click.echo(get_command_info(project_dir=ctx.obj["project_dir"]))


@nemory.command()
@click.pass_context
def init(ctx: Context) -> None:
    """
    Create an empty Nemory project
    """
    project_dir = ctx.obj["project_dir"]
    try:
        init_project_dir(project_dir=project_dir)
    except InitProjectError as e:
        if e.reason == InitErrorReason.PROJECT_DIR_DOESNT_EXIST:
            if click.confirm(
                f"The directory {ctx.obj['project_dir'].resolve()} does not exist. Do you want to create it?",
                default=True,
            ):
                create_project_dir(project_dir=project_dir)
                init_project_dir(project_dir=project_dir)
            else:
                return
        else:
            raise e

    click.echo(f"Project initialized successfully at {project_dir.resolve()}")

    try:
        resolve_ollama_bin()
    except RuntimeError as e:
        click.echo(str(e), err=True)

    if click.confirm("\nDo you want to configure a datasource now?"):
        add_datasource_config_internal(project_dir)


@nemory.group()
def datasource() -> None:
    pass


@datasource.command(name="add")
@click.pass_context
def add_datasource_config(ctx: Context) -> None:
    """
    Add a new datasource configuration, asking all relevant information for that datasource and saving it in your Nemory project.
    """
    add_datasource_config_internal(ctx.obj["project_dir"])


@datasource.command(name="validate")
@click.pass_context
def validate_datasource_config(ctx: Context) -> None:
    """
    Validates whether a datasource configuration is valid and the connection with the datasource can be established.
    """
    validate_datasource_config_internal(ctx.obj["project_dir"])


@nemory.command()
@click.pass_context
def build(ctx: Context) -> None:
    build_all_datasources(project_dir=ctx.obj["project_dir"])


@nemory.command()
@click.argument(
    "retrieve-text",
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
@click.option("-o", "--output-format", type=click.STRING, help="The output format [file (default), streamed]")
@click.pass_context
def retrieve(
    ctx: Context, retrieve_text: tuple[str, ...], run_name: str | None, limit: int | None, output_format: str = "file"
) -> None:
    """
    Search the project's built context for the most relevant chunks.
    """
    text = " ".join(retrieve_text)
    retrieve_embeddings(
        project_dir=ctx.obj["project_dir"],
        retrieve_text=text,
        run_name=run_name,
        limit=limit,
        output_format=output_format,
    )


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
    if transport == "stdio":
        configure_logging(verbose=False, quiet=True, project_dir=ctx.obj["project_dir"])
    run_mcp_server(project_dir=ctx.obj["project_dir"], run_name=run_name, transport=transport, host=host, port=port)
