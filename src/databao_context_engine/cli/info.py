import os
import sys
from pathlib import Path

import click

from databao_context_engine import DceInfo, get_databao_context_engine_info


def echo_info(project_dir: Path) -> None:
    click.echo(_generate_info_string(get_databao_context_engine_info(project_dir=project_dir)))


def _generate_info_string(command_info: DceInfo) -> str:
    info_lines = []
    info_lines.append(f"Databao context engine version: {command_info.version}")
    info_lines.append(f"Databao context engine storage dir: {command_info.dce_path}")

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
