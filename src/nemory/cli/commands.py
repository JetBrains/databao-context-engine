import click
from importlib.metadata import version

from click import Context


@click.group()
@click.option("-v", "--verbose", is_flag=True, help='Enable debug logging')
@click.option("-d", "--project-dir", type=click.STRING, default="./", help='Location of your Nemory project')
@click.pass_context
def nemory(ctx: Context, verbose: bool, project_dir: str) -> None:
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["project_dir"] = project_dir


@nemory.command()
@click.pass_context
def info(ctx: Context) -> None:
    """
    Display system-wide information
    """
    click.echo(f"Nemory version: {version('nemory')}")
    click.echo(f"is verbose? {ctx.obj["verbose"]}")
    click.echo(f"project dir: {ctx.obj["project_dir"]}")
