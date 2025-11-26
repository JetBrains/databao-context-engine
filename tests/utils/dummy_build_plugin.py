import uuid
from datetime import datetime
from typing import TypedDict, Mapping, Any

from nemory.pluginlib.build_plugin import (
    BuildDatasourcePlugin,
    BuildExecutionResult,
    EmbeddableChunk,
)


class DbTable(TypedDict):
    name: str
    description: str


class DbSchema(TypedDict):
    name: str
    description: str
    tables: list[DbTable]


def _convert_table_to_embedding_chunk(table: DbTable) -> EmbeddableChunk:
    return EmbeddableChunk(
        embeddable_text=f"{table['name']} - {table['description']}",
        content=table,
    )


class DummyBuildDatasourcePlugin(BuildDatasourcePlugin[Mapping[str, Any]]):
    id = "jetbrains/dummy"
    name = "Dummy Plugin"

    def supported_types(self) -> set[str]:
        return {"databases/dummy_db"}

    def execute(self, full_type: str, datasource_name: str, file_config: Mapping[str, Any]) -> BuildExecutionResult:
        return BuildExecutionResult(
            id=str(uuid.uuid4()),
            name=file_config["displayName"],
            type=full_type,
            description="My best description for that DB",
            executed_at=datetime.now(),
            version="1.0",
            result={
                "catalogs": [
                    {
                        "name": "random_catalog",
                        "description": "A great catalog",
                        "schemas": [
                            DbSchema(
                                name="a_schema",
                                description="The only schema",
                                tables=[
                                    DbTable(
                                        name="a_table",
                                        description="A table",
                                    ),
                                    DbTable(
                                        name="second_table",
                                        description="An other table",
                                    ),
                                ],
                            ),
                        ],
                    }
                ]
            },
        )

    def divide_result_into_chunks(self, build_result: BuildExecutionResult) -> list[EmbeddableChunk]:
        return [
            _convert_table_to_embedding_chunk(
                table=table,
            )
            for catalog in build_result.result.get("catalogs", list())
            for schema in catalog.get("schemas", list())
            for table in schema.get("tables", list())
        ]
