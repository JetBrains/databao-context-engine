import sys
from pathlib import Path
from typing import Literal

import click
from click import Context

from databao_context_engine.cli.datasources import add_datasource_config_cli, validate_datasource_config_cli
from databao_context_engine.cli.info import echo_info
from databao_context_engine.config.logging import configure_logging
from databao_context_engine.databao_context_project_manager import DatabaoContextProjectManager
from databao_context_engine.databao_engine import DatabaoContextEngine
from databao_context_engine.llm.install import resolve_ollama_bin
from databao_context_engine.mcp.mcp_runner import McpTransport, run_mcp_server
from databao_context_engine.project.init_project import InitErrorReason, InitProjectError, init_project_dir
from databao_context_engine.project.layout import create_project_dir
from databao_context_engine.project.types import DatasourceId
from databao_context_engine.services.chunk_embedding_service import ChunkEmbeddingMode
from databao_context_engine.storage.migrate import migrate


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging")
@click.option("-q", "--quiet", is_flag=True, help="Disable all console logging")
@click.option(
    "-d",
    "--project-dir",
    type=click.STRING,
    help="Location of your Databao Context Engine project",
)
@click.pass_context
def dce(ctx: Context, verbose: bool, quiet: bool, project_dir: str | None) -> None:
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


@dce.command()
@click.pass_context
def info(ctx: Context) -> None:
    """
    Display system-wide information
    """

    echo_info(ctx.obj["project_dir"])


@dce.command()
@click.pass_context
def init(ctx: Context) -> None:
    """
    Create an empty Databao Context Engine project
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
        add_datasource_config_cli(project_dir)


@dce.group()
def datasource() -> None:
    """
    Manage datasource configurations
    """
    pass


@datasource.command(name="add")
@click.pass_context
def add_datasource_config(ctx: Context) -> None:
    """
    Add a new datasource configuration.

    The command will ask all relevant information for that datasource and save it in your Databao Context Engine project.
    """
    add_datasource_config_cli(ctx.obj["project_dir"])


@datasource.command(name="check")
@click.argument(
    "datasources-config-files",
    type=click.STRING,
    nargs=-1,
)
@click.pass_context
def check_datasource_config(ctx: Context, datasources_config_files: list[str] | None) -> None:
    """
    Check whether a datasource configuration is valid.

    The configuration is considered as valid if a connection with the datasource can be established.

    By default, all datasources declared in the project will be checked.
    You can explicitely list which datasources to validate by using the [DATASOURCES_CONFIG_FILES] argument. Each argument must be the path to the file within the src folder (e.g: my-folder/my-config.yaml)
    """

    datasource_ids = (
        [DatasourceId.from_string_repr(datasource_config_file) for datasource_config_file in datasources_config_files]
        if datasources_config_files is not None
        else None
    )

    validate_datasource_config_cli(ctx.obj["project_dir"], datasource_ids=datasource_ids)


@dce.command()
@click.option(
    "-m",
    "--chunk-embedding-mode",
    type=click.Choice(
        ["embeddable_text_only", "generated_description_only", "embeddable_text_and_generated_description"]
    ),
    default="embeddable_text_only",
    help="Choose how chunks will be embedded. If a mode with the generated_description is selected, a local LLM model will be downloaded and used.",
)
@click.pass_context
def build(
    ctx: Context,
    chunk_embedding_mode: Literal[
        "embeddable_text_only", "generated_description_only", "embeddable_text_and_generated_description"
    ],
) -> None:
    """
    Build context for all datasources

    The output of the build command will be saved in a "run" folder in the output directory.

    Internally, this indexes the context to be used by the MCP server and the "retrieve" command.
    """
    result = DatabaoContextProjectManager(project_dir=ctx.obj["project_dir"]).build_context(
        datasource_ids=None, chunk_embedding_mode=ChunkEmbeddingMode(chunk_embedding_mode.upper())
    )

    click.echo(f"Build complete. Processed {len(result)} datasources.")


@dce.command()
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
@click.option(
    "-o",
    "--output-format",
    type=click.Choice(["file", "streamed"]),
    default="file",
    help="The output format [file (default), streamed]",
)
@click.pass_context
def retrieve(
    ctx: Context,
    retrieve_text: tuple[str, ...],
    run_name: str | None,
    limit: int | None,
    output_format: Literal["file", "streamed"],
) -> None:
    """
    Search the project's built context for the most relevant chunks.
    """
    text = " ".join(retrieve_text)

    databao_engine = DatabaoContextEngine(project_dir=ctx.obj["project_dir"])

    retrieve_results = databao_engine.search_context(
        retrieve_text=text, run_name=run_name, limit=limit, export_to_file=output_format == "file"
    )

    if output_format == "streamed":
        display_texts = [context_search_result.context_result for context_search_result in retrieve_results]
        click.echo("\n".join(display_texts))


@dce.command()
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
    Run Databao Context Engine's MCP server
    """
    if transport == "stdio":
        configure_logging(verbose=False, quiet=True, project_dir=ctx.obj["project_dir"])
    run_mcp_server(project_dir=ctx.obj["project_dir"], run_name=run_name, transport=transport, host=host, port=port)
