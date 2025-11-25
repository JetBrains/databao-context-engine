from pathlib import Path

import yaml


def export_retrieve_results(run_dir: Path, retrieve_results: list[str]):
    path = run_dir.joinpath("context_duckdb.yaml")

    with path.open("w") as export_file:
        for result in retrieve_results:
            yaml.safe_dump(result, export_file)
            export_file.write("\n")
