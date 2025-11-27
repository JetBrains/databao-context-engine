import uuid
from datetime import datetime
from typing import TypedDict, Mapping, Any

from nemory.pluginlib.build_plugin import (
    BuildDatasourcePlugin,
    BuildExecutionResult,
    EmbeddableChunk,
    DefaultBuildDatasourcePlugin,
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
    id = "jetbrains/dummy_db"
    name = "Dummy DB Plugin"
    config_file_type: Mapping[str, Any] = Mapping[str, Any]

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


class DummyDefaultDatasourcePlugin(DefaultBuildDatasourcePlugin):
    id = "jetbrains/dummy_default"
    name = "Dummy Plugin with a default type"

    def supported_types(self) -> set[str]:
        return {"dummy/dummy_default"}

    def execute(self, full_type: str, datasource_name: str, file_config: dict[str, Any]) -> BuildExecutionResult:
        return BuildExecutionResult(
            id="dummy",
            name=datasource_name,
            type=full_type,
            result={"ok": True},
            version="1.0",
            executed_at=datetime.now(),
            description=None,
        )

    def divide_result_into_chunks(self, build_result: BuildExecutionResult) -> list[EmbeddableChunk]:
        return []
