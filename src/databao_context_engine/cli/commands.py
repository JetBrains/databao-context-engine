import os
import sys
from pathlib import Path
from typing import Literal, Sequence

import click
from click import Context

from databao_context_engine import (
    ChunkEmbeddingMode,
    DatabaoContextEngine,
    DatabaoContextProjectManager,
    DatasourceId,
    DatasourceStatus,
)
from databao_context_engine.cli.datasources import (
    run_sql_query_cli,
)
from databao_context_engine.cli.info import echo_info
from databao_context_engine.config.logging import configure_logging
from databao_context_engine.mcp.mcp_runner import McpTransport, run_mcp_server


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
        click.echo("Arguments --quiet and --verbose can not be used together", file=sys.stderr)
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


@dce.command()
@click.pass_context
def info(ctx: Context) -> None:
    """Display system-wide information."""
    echo_info(ctx.obj["project_dir"])


@dce.group()
def datasource() -> None:
    """Manage datasource configurations."""
    pass


@datasource.command(name="run_sql")
@click.argument(
    "datasource-config-file",
    type=click.STRING,
)
@click.argument(
    "sql",
    type=click.STRING,
)
@click.pass_context
def run_sql_query(ctx: Context, datasource_config_file: str, sql: str) -> None:
    datasource_id = DatasourceId.from_string_repr(datasource_config_file)
    run_sql_query_cli(ctx.obj["project_dir"], datasource_id=datasource_id, sql=sql)


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
@click.option(
    "--should-index/--should-not-index",
    default=True,
    show_default=True,
    help="Whether to index the context. If disabled, the context will be built but not indexed.",
)
@click.pass_context
def build(
    ctx: Context,
    chunk_embedding_mode: Literal[
        "embeddable_text_only", "generated_description_only", "embeddable_text_and_generated_description"
    ],
    should_index: bool,
) -> None:
    """Build context for all datasources.

    The output of the build command will be saved in the output directory.

    Internally, this indexes the context to be used by the MCP server and the "retrieve" command.
    """
    results = DatabaoContextProjectManager(project_dir=ctx.obj["project_dir"]).build_context(
        datasource_ids=None,
        chunk_embedding_mode=ChunkEmbeddingMode(chunk_embedding_mode.upper()),
        should_index=should_index,
    )

    _echo_operation_result(
        heading="Build complete",
        verb="Processed",
        noun="datasource(s)",
        results=results,
    )


@dce.command()
@click.argument(
    "datasources-config-files",
    nargs=-1,
    type=click.STRING,
)
@click.pass_context
def index(ctx: Context, datasources_config_files: tuple[str, ...]) -> None:
    """Index and create embeddings for built context files into duckdb.

    If one or more datasource config files are provided, only those datasources will be indexed.
    If no paths are provided, all built contexts found in the output directory will be indexed.
    """
    datasource_ids = (
        [DatasourceId.from_string_repr(p) for p in datasources_config_files] if datasources_config_files else None
    )

    results = DatabaoContextProjectManager(project_dir=ctx.obj["project_dir"]).index_built_contexts(
        datasource_ids=datasource_ids
    )

    _echo_operation_result(
        heading="Indexing complete",
        verb="Indexed",
        noun="datasource(s)",
        results=results,
    )


@dce.command()
@click.argument(
    "retrieve-text",
    nargs=-1,
    required=True,
)
@click.option(
    "-l",
    "--limit",
    type=click.INT,
    help="Maximum number of chunk matches to return.",
)
@click.option(
    "-o",
    "--output-file",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    help="If provided, the results are written as YAML into the file given as a path, rather than printed to the console.",
)
@click.option(
    "-d",
    "--datasource-id",
    "datasource_ids_as_str",
    type=click.STRING,
    multiple=True,
    help="A datasourceId to restrict the search to. If not provided, search across all datasources. This option can be provided multiple times",
)
@click.pass_context
def retrieve(
    ctx: Context,
    retrieve_text: tuple[str, ...],
    limit: int | None,
    output_file: str | None,
    datasource_ids_as_str: tuple[str, ...] | None,
) -> None:
    """Search the project's built context for the most relevant chunks."""
    text = " ".join(retrieve_text)

    datasource_ids = (
        [DatasourceId.from_string_repr(datasource_id) for datasource_id in datasource_ids_as_str]
        if datasource_ids_as_str is not None
        else None
    )

    databao_engine = DatabaoContextEngine(project_dir=ctx.obj["project_dir"])

    retrieve_results = databao_engine.search_context(retrieve_text=text, limit=limit, datasource_ids=datasource_ids)

    display_texts = [context_search_result.context_result for context_search_result in retrieve_results]
    if output_file is not None:
        Path(output_file).expanduser().write_text(f"---{os.linesep}".join(display_texts))
        click.echo(f"Found {len(retrieve_results)} results, written to {output_file}")
    else:
        click.echo("\n".join(display_texts))


@dce.command()
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
def mcp(ctx: Context, host: str | None, port: int | None, transport: McpTransport) -> None:
    """Run Databao Context Engine's MCP server."""
    if transport == "stdio":
        configure_logging(verbose=False, quiet=True, project_dir=ctx.obj["project_dir"])
    run_mcp_server(project_dir=ctx.obj["project_dir"], transport=transport, host=host, port=port)


def _echo_operation_result(
    *,
    heading: str,
    verb: str,
    noun: str,
    results: Sequence,
) -> None:
    total = len(results)
    ok = sum(1 for r in results if r.status == DatasourceStatus.OK)
    skipped = sum(1 for r in results if r.status == DatasourceStatus.SKIPPED)
    failed = sum(1 for r in results if r.status == DatasourceStatus.FAILED)

    suffix_parts: list[str] = []
    if skipped:
        suffix_parts.append(f"skipped {skipped}")
    if failed:
        suffix_parts.append(f"failed {failed}")

    suffix = f" ({', '.join(suffix_parts)})" if suffix_parts else ""
    click.echo(f"{heading}. {verb} {ok}/{total} {noun}{suffix}.")
    if failed == 0:
        return

    failed_results = [r for r in results if r.status == DatasourceStatus.FAILED]
    if not failed_results:
        return

    click.echo("Failed:")
    for r in failed_results:
        datasource = r.datasource_id.relative_path_to_config_file()
        message = (r.error or "").strip() or "(no error message)"
        click.echo(f"  - {datasource}: {message}")
