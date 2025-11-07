import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypedDict

from nemory.features.build_sources.plugin_lib.build_plugin import (
    BuildPlugin,
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
        content=table,  # type: ignore[arg-type]
    )


@dataclass
class DummyBuildResult(BuildExecutionResult):
    id: str
    name: str
    type: str
    description: str
    executed_at: datetime
    version: str
    result: dict[str, Any]

    def get_chunks(self) -> list[EmbeddableChunk]:
        return [
            _convert_table_to_embedding_chunk(
                table=table,
            )
            for catalog in self.result.get("catalogs", list())
            for schema in catalog.get("schemas", list())
            for table in schema.get("tables", list())
        ]


class DummyBuildPlugin(BuildPlugin):
    def supported_types(self) -> set[str]:
        return {"databases/dummy_db"}

    def execute(self, full_type: str, file_config: dict[str, Any]) -> BuildExecutionResult:
        return DummyBuildResult(
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
