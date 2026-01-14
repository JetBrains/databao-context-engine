import os
import sys
from pathlib import Path

import click

from nemory.project.info import NemoryInfo, get_command_info


def echo_info(project_dir: Path) -> None:
    click.echo(_generate_info_string(get_command_info(project_dir=project_dir)))


def _generate_info_string(command_info: NemoryInfo) -> str:
    info_lines = []
    info_lines.append(f"Nemory version: {command_info.version}")
    info_lines.append(f"Nemory storage dir: {command_info.nemory_path}")

    info_lines.append("")

    info_lines.append(f"OS name: {sys.platform}")
    info_lines.append(f"OS architecture: {os.uname().machine if hasattr(os, 'uname') else 'unknown'}")

    info_lines.append("")

    if command_info.project_info.is_initialised:
        info_lines.append(f"Project dir: {command_info.project_info.project_path.resolve()}")
        info_lines.append(f"Project ID: {str(command_info.project_info.project_id)}")
    else:
        info_lines.append(f"Project not initialised at {command_info.project_info.project_path.resolve()}")

    return os.linesep.join(info_lines)
