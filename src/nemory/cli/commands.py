from importlib.metadata import version
from pathlib import Path

import click
from click import Context

from nemory.build_sources.public.api import build_all_datasources
from nemory.config.logging import configure_logging
from nemory.project.init_project import init_project_dir
from nemory.project.layout import read_config_file
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
