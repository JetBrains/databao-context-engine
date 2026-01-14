from pathlib import Path

from nemory.project.layout import ALL_RESULTS_FILE_NAME
from nemory.project.runs import get_run_dir


def run_all_results_tool(project_dir: Path, run_name: str) -> str:
    run_directory = get_run_dir(project_dir, run_name)

    with open(run_directory.joinpath(ALL_RESULTS_FILE_NAME), "r") as file:
        return file.read()
