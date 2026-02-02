import os

from jinja2.sandbox import SandboxedEnvironment

from databao_context_engine.project.layout import ProjectLayout


class DceTemplateError(Exception):
    pass


class UnknownEnvVarTemplateError(DceTemplateError):
    pass


def render_template(project_layout: ProjectLayout, source: str) -> str:
    env = SandboxedEnvironment()

    return env.from_string(source=str(source)).render(
        env_var=resolve_env_var,
        PROJECT_DIR=project_layout.project_dir.resolve(),
        SRC_DIR=project_layout.src_dir.resolve(),
    )


def resolve_env_var(env_var: str, default: str | None = None) -> str:
    if env_var in os.environ:
        return os.environ[env_var]

    if default is not None:
        return default

    raise UnknownEnvVarTemplateError(
        f"Error in template. The environment variable {env_var} is missing and no default was provided"
    )
