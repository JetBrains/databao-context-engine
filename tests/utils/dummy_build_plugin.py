import uuid
from dataclasses import dataclass
from datetime import datetime
from io import BufferedReader
from typing import Any, Mapping, TypedDict

from nemory.pluginlib.build_plugin import (
    BuildDatasourcePlugin,
    BuildExecutionResult,
    BuildFilePlugin,
    BuildPlugin,
    DefaultBuildDatasourcePlugin,
    EmbeddableChunk,
)
from nemory.pluginlib.config_properties import ConfigPropertyDefinition


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


@dataclass
class DummyConfigNested:
    nested_field: str
    other_nested_property: int
    optional_with_default: str = "optional_default"


class DummyConfigFileType(TypedDict):
    type: str
    name: str
    other_property: float
    property_with_default: str
    nested_dict: DummyConfigNested


class DummyBuildDatasourcePlugin(BuildDatasourcePlugin[DummyConfigFileType]):
    id = "jetbrains/dummy_db"
    name = "Dummy DB Plugin"
    config_file_type = DummyConfigFileType

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

    def get_mandatory_config_file_structure(self) -> list[ConfigPropertyDefinition]:
        return [
            ConfigPropertyDefinition(property_key="other_property", required=True, property_type=float),
            ConfigPropertyDefinition(
                property_key="property_with_default", required=True, default_value="default_value"
            ),
            ConfigPropertyDefinition(
                property_key="nested_dict",
                required=True,
                nested_properties=[
                    ConfigPropertyDefinition(property_key="nested_field", required=True),
                    ConfigPropertyDefinition(property_key="other_nested_property", required=False),
                    ConfigPropertyDefinition(
                        property_key="optional_with_default",
                        required=False,
                        property_type=int,
                        default_value="1111",
                    ),
                ],
            ),
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


class DummyFilePlugin(BuildFilePlugin):
    id = "jetbrains/dummy_file"
    name = "Dummy Plugin with a default type"

    def supported_types(self) -> set[str]:
        return {"files/dummy"}

    def execute(self, full_type: str, file_name: str, file_buffer: BufferedReader) -> BuildExecutionResult:
        return BuildExecutionResult(
            id="dummy",
            name=file_name,
            type=full_type,
            result={"file_ok": True},
            version="1.0",
            executed_at=datetime.now(),
            description=None,
        )

    def divide_result_into_chunks(self, build_result: BuildExecutionResult) -> list[EmbeddableChunk]:
        return []


class AdditionalDummyConfigFile(TypedDict):
    type: str
    other_field: str


class AdditionalDummyPlugin(BuildDatasourcePlugin[AdditionalDummyConfigFile]):
    id = "additional/dummy"
    name = "Additional Dummy Plugin"
    config_file_type = AdditionalDummyConfigFile

    def supported_types(self) -> set[str]:
        return {"additional/dummy_type"}

    def execute(
        self, full_type: str, datasource_name: str, file_config: AdditionalDummyConfigFile
    ) -> BuildExecutionResult:
        return BuildExecutionResult(
            id="dummy",
            name=datasource_name,
            type=full_type,
            result={"additional_ok": True},
            version="1.0",
            executed_at=datetime.now(),
            description=None,
        )

    def divide_result_into_chunks(self, build_result: BuildExecutionResult) -> list[EmbeddableChunk]:
        return []


def load_dummy_plugins(exclude_file_plugins: bool = False) -> dict[str, BuildPlugin]:
    result: dict[str, BuildPlugin] = {
        "databases/dummy_db": DummyBuildDatasourcePlugin(),
        "dummy/dummy_default": DummyDefaultDatasourcePlugin(),
        "additional/dummy_type": AdditionalDummyPlugin(),
    }

    if not exclude_file_plugins:
        result.update({"files/dummy": DummyFilePlugin()})

    return result
