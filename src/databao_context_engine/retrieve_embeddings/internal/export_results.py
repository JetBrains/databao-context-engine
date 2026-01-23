from pathlib import Path


def export_retrieve_results(output_dir: Path, retrieve_results: list[str]) -> Path:
    path = output_dir.joinpath("context_duckdb.yaml")

    with path.open("w") as export_file:
        for result in retrieve_results:
            export_file.write(result)
            export_file.write("\n")

    return path
