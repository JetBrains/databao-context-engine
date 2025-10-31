from pathlib import Path

import yaml

from nemory.features.build_sources.internal.connections import duckdb_connection


def build_all_datasources(project_dir: str, property_file_path: str | None) -> None:
    print("Internal build_all_datasources function called")

    if property_file_path is None:
        print("No property_file_path provided")
        return

    with open(property_file_path, "r") as property_file:
        try:
            properties = yaml.safe_load(property_file)

            print(f"Read properties from {property_file_path}")
            print(properties)

            if properties["type"] == "duckdb":
                filename = properties["fileName"]
                duckdb_file = str(Path(project_dir).joinpath(filename).absolute())
                connection = duckdb_connection.connect(duckdb_file)
                connection.execute("SELECT database_name FROM duckdb_databases()")
                database_names_result = connection.fetchall()
                # The DB connection returns a tuple for each row
                database_names = [result[0] for result in database_names_result]

                ignored_databases = {"system", "temp"}
                for database_name in database_names:
                    if database_name not in ignored_databases:
                        connection.execute("SELECT schema_name FROM information_schema.schemata WHERE catalog_name = ?", [database_name])
                        schema_names_result = connection.fetchall()
                        # The DB connection returns a tuple for each row
                        schema_names = [result[0] for result in schema_names_result]
                        print(f"Schemas in {database_name}: {schema_names}")

        except yaml.YAMLError as exc:
            print(exc)
