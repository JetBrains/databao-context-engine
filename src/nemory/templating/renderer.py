import os

from jinja2.sandbox import SandboxedEnvironment


class DceTemplateError(Exception):
    pass


def render_template(source: str) -> str:
    env = SandboxedEnvironment()

    try:
        return env.from_string(source=str(source)).render()
    except Exception as e:
        # Wraps any Jinja exception into our own
        raise DceTemplateError(f"Error rendering template:{os.linesep}{str(e)}") from e
