from pathlib import Path


def export_retrieve_results(run_dir: Path, retrieve_results: list[str]) -> Path:
    path = run_dir.joinpath("context_duckdb.yaml")

    with path.open("w") as export_file:
        for result in retrieve_results:
            export_file.write(result)
            export_file.write("\n")

    return path
