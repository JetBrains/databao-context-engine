import duckdb


def connect(database_path: str):
    return duckdb.connect(database=database_path, read_only=True)
