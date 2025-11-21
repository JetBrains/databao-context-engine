from pathlib import Path

import yaml


def export_query_results(run_dir: Path, query_results: list[str]):
    path = run_dir.joinpath("context_duckdb.yaml")

    with path.open("w") as export_file:
        for result in query_results:
            yaml.safe_dump(result, export_file)
            export_file.write("\n")
