import os
import sys
from pathlib import Path

import click

from databao_context_engine import DceInfo, DceProjectInfo, get_databao_context_engine_info
from databao_context_engine.project.info import get_databao_context_engine_project_info


def echo_info(project_dir: Path) -> None:
    dce_info = get_databao_context_engine_info()
    dce_project_info = get_databao_context_engine_project_info(project_dir=project_dir)
    click.echo(_generate_info_string(dce_info, dce_project_info))


def _generate_info_string(dce_info: DceInfo, project_info: DceProjectInfo) -> str:
    info_lines = []
    info_lines.append(f"Databao context engine version: {dce_info.version}")
    info_lines.append(f"Databao context engine storage dir: {dce_info.dce_path}")
    info_lines.append(f"Databao context engine plugins: {dce_info.plugin_ids}")

    info_lines.append("")

    info_lines.append(f"OS name: {sys.platform}")
    info_lines.append(f"OS architecture: {os.uname().machine if hasattr(os, 'uname') else 'unknown'}")

    info_lines.append("")

    if project_info.is_initialized:
        info_lines.append(f"Project dir: {project_info.project_path.resolve()}")
        info_lines.append(f"Project ID: {str(project_info.project_id)}")
    else:
        info_lines.append(f"Project not initialized at {project_info.project_path.resolve()}")

    return os.linesep.join(info_lines)
