import uuid
from dataclasses import dataclass
from io import BufferedReader
from typing import Annotated, Any, Mapping, TypedDict

from databao_context_engine.pluginlib.build_plugin import (
    BuildDatasourcePlugin,
    BuildFilePlugin,
    BuildPlugin,
    DatasourceType,
    DefaultBuildDatasourcePlugin,
    EmbeddableChunk,
)
from databao_context_engine.pluginlib.config import (
    ConfigPropertyAnnotation,
    ConfigPropertyDefinition,
    CustomiseConfigProperties,
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


@dataclass
class DummyConfigNested:
    nested_field: str
    other_nested_property: int
    optional_with_default: Annotated[int, ConfigPropertyAnnotation(default_value="1111", required=False)] = 1111


class DummyConfigFileType(TypedDict):
    type: str
    name: str
    other_property: float
    property_with_default: Annotated[str, ConfigPropertyAnnotation(default_value="default_value", required=True)]
    nested_dict: DummyConfigNested
    ignored_dict: dict[str, str]


class DummyBuildDatasourcePlugin(BuildDatasourcePlugin[DummyConfigFileType]):
    id = "jetbrains/dummy_db"
    name = "Dummy DB Plugin"
    config_file_type = DummyConfigFileType

    def supported_types(self) -> set[str]:
        return {"databases/dummy_db"}

    def build_context(self, full_type: str, datasource_name: str, file_config: Mapping[str, Any]) -> Any:
        return {
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
        }

    def divide_context_into_chunks(self, context: Any) -> list[EmbeddableChunk]:
        return [
            _convert_table_to_embedding_chunk(
                table=table,
            )
            for catalog in context.get("catalogs", list())
            for schema in catalog.get("schemas", list())
            for table in schema.get("tables", list())
        ]


class DummyDefaultDatasourcePlugin(DefaultBuildDatasourcePlugin):
    id = "jetbrains/dummy_default"
    name = "Dummy Plugin with a default type"

    def supported_types(self) -> set[str]:
        return {"dummy/dummy_default"}

    def build_context(self, full_type: str, datasource_name: str, file_config: dict[str, Any]) -> Any:
        return {"ok": True}

    def divide_context_into_chunks(self, context: Any) -> list[EmbeddableChunk]:
        return []


class DummyFilePlugin(BuildFilePlugin):
    id = "jetbrains/dummy_file"
    name = "Dummy Plugin with a default type"

    def supported_types(self) -> set[str]:
        return {"files/dummy"}

    def build_file_context(self, full_type: str, file_name: str, file_buffer: BufferedReader) -> Any:
        return {"file_ok": True}

    def divide_context_into_chunks(self, context: Any) -> list[EmbeddableChunk]:
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

    def build_context(self, full_type: str, datasource_name: str, file_config: AdditionalDummyConfigFile) -> Any:
        return {"additional_ok": True}

    def divide_context_into_chunks(self, context: Any) -> list[EmbeddableChunk]:
        return []


class DummyPluginWithNoConfigType(DefaultBuildDatasourcePlugin, CustomiseConfigProperties):
    id = "dummy/no_config_type"
    name = "Dummy Plugin With No Config Type"

    def supported_types(self) -> set[str]:
        return {"dummy/no_config_type"}

    def build_context(self, full_type: str, datasource_name: str, file_config: dict[str, Any]) -> Any:
        return {"no_config_ok": True}

    def divide_context_into_chunks(self, context: Any) -> list[EmbeddableChunk]:
        return []

    def get_config_file_properties(self) -> list[ConfigPropertyDefinition]:
        return [
            ConfigPropertyDefinition(property_key="float_property", required=True, property_type=float),
            ConfigPropertyDefinition(
                property_key="nested_with_only_optionals",
                required=False,
                property_type=None,
                nested_properties=[
                    ConfigPropertyDefinition(property_key="optional_field", required=False, property_type=uuid.UUID),
                    ConfigPropertyDefinition(property_key="nested_field", required=False),
                ],
            ),
            ConfigPropertyDefinition(
                property_key="nested_dict",
                required=True,
                nested_properties=[
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


def load_dummy_plugins(exclude_file_plugins: bool = False) -> dict[DatasourceType, BuildPlugin]:
    result: dict[DatasourceType, BuildPlugin] = {
        DatasourceType(full_type="databases/dummy_db"): DummyBuildDatasourcePlugin(),
        DatasourceType(full_type="dummy/dummy_default"): DummyDefaultDatasourcePlugin(),
        DatasourceType(full_type="additional/dummy_type"): AdditionalDummyPlugin(),
        DatasourceType(full_type="dummy/no_config_type"): DummyPluginWithNoConfigType(),
    }

    if not exclude_file_plugins:
        result.update({DatasourceType(full_type="files/dummy"): DummyFilePlugin()})

    return result
