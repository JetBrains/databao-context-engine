import os
import sys
from importlib.metadata import version
from pathlib import Path

from nemory.project.layout import is_project_dir_valid, read_config_file
from nemory.system.properties import get_nemory_path


def get_command_info(project_dir: str) -> str:
    info_lines = []
    info_lines.append(f"Nemory version: {get_nemory_version()}")
    info_lines.append(f"Nemory storage dir: {get_nemory_path()}")

    info_lines.append("")

    info_lines.append(f"OS name: {sys.platform}")
    info_lines.append(f"OS architecture: {os.uname().machine if hasattr(os, 'uname') else 'unknown'}")

    info_lines.append("")

    project_path = Path(project_dir)
    if is_project_dir_valid(project_path):
        info_lines.append(f"Project dir: {project_dir}")
        info_lines.append(f"Project ID: {read_config_file(project_path).project_id}")
    else:
        info_lines.append(f"Project not initialised at {project_dir}")

    return os.linesep.join(info_lines)


def get_nemory_version() -> str:
    return version("nemory")
