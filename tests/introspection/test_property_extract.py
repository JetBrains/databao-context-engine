from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Annotated, Any, Collection, Mapping, Optional, TypedDict
from uuid import UUID

from pydantic import BaseModel, Field
from pytest_unordered import unordered

from nemory.introspection.property_extract import get_property_list_from_type
from nemory.pluginlib.config_properties import ConfigPropertyAnnotation, ConfigPropertyDefinition


class TestSubclass:
    union_type: str | None
    other_property: "float"
    uuid: UUID

    def add_callable(self, param_1: int) -> int:  # type: ignore[empty-body]
        pass


@dataclass
class NestedSubclass:
    some_property: bool
    ignored_dict: dict[str, int]


class NestedBaseModel(BaseModel):
    my_property: str = Field(default="My default value")


class TestPydanticBaseModel(BaseModel):
    regular_property: str
    nested_model: NestedBaseModel


class SecondSubclass(TypedDict):
    nested_subclass: "NestedSubclass | None"
    other_property: float
    uuid: UUID
    ignored_list: list[UUID]
    nested_pydantic_model: TestPydanticBaseModel


@dataclass
class TestDataclass:
    complex: TestSubclass
    required: Annotated[datetime, ConfigPropertyAnnotation(required=True)]
    with_default_value: Annotated[date, ConfigPropertyAnnotation(default_value=date(2025, 12, 4).isoformat())]
    optional_subclass: Optional[SecondSubclass]
    ignored_property: Annotated[TestSubclass, ConfigPropertyAnnotation(ignored_for_config_wizard=True)]
    ignored_tuple: tuple[int, ...]
    a: int = field(default=1)
    b: float = 3.14
    """
    Documented attribute
    """

    def fun(self):
        pass


def test_get_property_list_from_type__with_dataclass():
    property_list = get_property_list_from_type(TestDataclass)

    assert property_list == unordered(
        ConfigPropertyDefinition(property_key="a", required=False, property_type=int, default_value="1"),
        ConfigPropertyDefinition(property_key="b", required=False, property_type=float, default_value="3.14"),
        ConfigPropertyDefinition(
            property_key="complex",
            required=True,
            property_type=None,
            nested_properties=[
                ConfigPropertyDefinition(property_key="union_type", required=False, property_type=str),
                ConfigPropertyDefinition(property_key="other_property", required=False, property_type=float),
                ConfigPropertyDefinition(property_key="uuid", required=False, property_type=UUID),
            ],
        ),
        ConfigPropertyDefinition(property_key="required", required=True, property_type=datetime),
        ConfigPropertyDefinition(
            property_key="with_default_value", required=False, default_value="2025-12-04", property_type=date
        ),
        ConfigPropertyDefinition(
            property_key="optional_subclass",
            required=True,
            property_type=None,
            nested_properties=[
                ConfigPropertyDefinition(
                    property_key="nested_subclass",
                    required=False,
                    property_type=None,
                    nested_properties=[
                        ConfigPropertyDefinition(property_key="some_property", required=True, property_type=bool)
                    ],
                ),
                ConfigPropertyDefinition(property_key="other_property", required=False, property_type=float),
                ConfigPropertyDefinition(property_key="uuid", required=False, property_type=UUID),
                ConfigPropertyDefinition(
                    property_key="nested_pydantic_model",
                    required=False,
                    property_type=None,
                    nested_properties=[
                        ConfigPropertyDefinition(property_key="regular_property", required=True, property_type=str),
                        ConfigPropertyDefinition(
                            property_key="nested_model",
                            required=True,
                            property_type=None,
                            nested_properties=[
                                ConfigPropertyDefinition(
                                    property_key="my_property",
                                    required=False,
                                    property_type=str,
                                    default_value="My default value",
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        ),
    )


def test_get_property_list__from_scalar():
    assert get_property_list_from_type(str) == []
    assert get_property_list_from_type(int) == []
    assert get_property_list_from_type(dict[Any, bool]) == []
    assert get_property_list_from_type(Mapping[str, int]) == []
    assert get_property_list_from_type(set[UUID]) == []
    assert get_property_list_from_type(list[float]) == []
    assert get_property_list_from_type(Collection[Any]) == []
    assert get_property_list_from_type(tuple[date, datetime, float]) == []


@dataclass(kw_only=True)
class DataclassWithAllCases:
    regular_property: int
    regular_property_with_default: bool = True
    property_with_field_default: bool = field(default=False)
    property_with_annotated_default: Annotated[bool, ConfigPropertyAnnotation(default_value="True", required=True)]
    property_with_annotated_default_and_default: Annotated[bool, ConfigPropertyAnnotation(default_value="True")] = False
    property_with_annotated_default_and_field_default: Annotated[
        bool, ConfigPropertyAnnotation(default_value="True")
    ] = field(default=False)
    property_with_string_type: "str"
    property_with_union_type_as_string: "int | None"


def test_get_property_list__from_dataclass():
    assert get_property_list_from_type(DataclassWithAllCases) == unordered(
        [
            ConfigPropertyDefinition(property_key="regular_property", required=True, property_type=int),
            ConfigPropertyDefinition(
                property_key="regular_property_with_default", required=False, property_type=bool, default_value="True"
            ),
            ConfigPropertyDefinition(
                property_key="property_with_field_default", required=False, property_type=bool, default_value="False"
            ),
            ConfigPropertyDefinition(
                property_key="property_with_annotated_default", required=True, property_type=bool, default_value="True"
            ),
            ConfigPropertyDefinition(
                property_key="property_with_annotated_default_and_default",
                required=False,
                property_type=bool,
                default_value="True",
            ),
            ConfigPropertyDefinition(
                property_key="property_with_annotated_default_and_field_default",
                required=False,
                property_type=bool,
                default_value="True",
            ),
            ConfigPropertyDefinition(
                property_key="property_with_string_type",
                required=True,
                property_type=str,
            ),
            ConfigPropertyDefinition(
                property_key="property_with_union_type_as_string",
                required=True,
                property_type=int,
            ),
        ]
    )


class BaseModelWithAllCases(BaseModel):
    regular_property: str
    regular_property_with_default: bool = False
    property_with_field_info: int = Field(description="This is a description")
    property_with_field_default: int = Field(default=1)
    property_with_annotated_default: Annotated[int, ConfigPropertyAnnotation(default_value="1", required=True)]
    property_with_annotated_default_and_default: Annotated[int, ConfigPropertyAnnotation(default_value="1")] = 2
    property_with_annotated_default_and_field_default: Annotated[int, ConfigPropertyAnnotation(default_value="1")] = (
        Field(default=2)
    )
    property_with_string_type: "str"
    property_with_union_type_as_string: "float | None"


def test_get_property_list__from_pydantic_base_model():
    assert get_property_list_from_type(BaseModelWithAllCases) == unordered(
        [
            ConfigPropertyDefinition(property_key="regular_property", required=True, property_type=str),
            ConfigPropertyDefinition(
                property_key="regular_property_with_default", required=False, property_type=bool, default_value="False"
            ),
            ConfigPropertyDefinition(property_key="property_with_field_info", required=True, property_type=int),
            ConfigPropertyDefinition(
                property_key="property_with_field_default",
                required=False,
                property_type=int,
                default_value="1",
            ),
            ConfigPropertyDefinition(
                property_key="property_with_annotated_default",
                required=True,
                property_type=int,
                default_value="1",
            ),
            ConfigPropertyDefinition(
                property_key="property_with_annotated_default_and_default",
                required=False,
                property_type=int,
                default_value="1",
            ),
            ConfigPropertyDefinition(
                property_key="property_with_annotated_default_and_field_default",
                required=False,
                property_type=int,
                default_value="1",
            ),
            ConfigPropertyDefinition(
                property_key="property_with_string_type",
                required=True,
                property_type=str,
            ),
            ConfigPropertyDefinition(
                property_key="property_with_union_type_as_string",
                required=True,
                property_type=float,
            ),
        ]
    )
