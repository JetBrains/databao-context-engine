import os

from jinja2.sandbox import SandboxedEnvironment


class DceTemplateError(Exception):
    pass


class UnknownEnvVarTemplateError(DceTemplateError):
    pass


def render_template(source: str) -> str:
    env = SandboxedEnvironment()

    return env.from_string(source=str(source)).render(env_var=resolve_env_var)


def resolve_env_var(env_var: str, default: str | None = None) -> str:
    if env_var in os.environ:
        return os.environ[env_var]

    if default is not None:
        return default

    raise UnknownEnvVarTemplateError(
        f"Error in template. The environment variable {env_var} is missing and no default was provided"
    )
