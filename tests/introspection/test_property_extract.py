from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Annotated, Optional, TypedDict
from uuid import UUID

from pydantic import BaseModel, Field
from pytest_unordered import unordered

from nemory.introspection.property_extract import get_property_list_from_type
from nemory.pluginlib.config_properties import ConfigPropertyAnnotation, ConfigPropertyDefinition


class TestSubclass:
    union_type: str | None
    other_property: float
    uuid: UUID

    def add_callable(self, param_1: int) -> int:  # type: ignore[empty-body]
        pass


@dataclass
class NestedSubclass:
    some_property: bool
    ignored_dict: dict[str, int]


class TestPydanticBaseModel(BaseModel):
    regular_property: str
    bool_with_default: bool = False
    property_with_field_info: int = Field(description="This is a description")
    property_with_default_in_field_info: str = Field(default="My default value")
    custom_annotation: Annotated[float, ConfigPropertyAnnotation(default_value="1.234", required=True)] = Field(
        default=2.345
    )


class SecondSubclass(TypedDict):
    nested_subclass: NestedSubclass | None
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
                            property_key="bool_with_default", required=False, property_type=bool, default_value="False"
                        ),
                        ConfigPropertyDefinition(
                            property_key="property_with_field_info", required=True, property_type=int
                        ),
                        ConfigPropertyDefinition(
                            property_key="property_with_default_in_field_info",
                            required=False,
                            property_type=str,
                            default_value="My default value",
                        ),
                        ConfigPropertyDefinition(
                            property_key="custom_annotation",
                            required=True,
                            property_type=float,
                            default_value="1.234",
                        ),
                    ],
                ),
            ],
        ),
    )
