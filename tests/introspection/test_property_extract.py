from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Annotated, Optional, TypedDict
from uuid import UUID

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


class SecondSubclass(TypedDict):
    nested_subclass: NestedSubclass | None
    other_property: float
    uuid: UUID
    ignored_list: list[UUID]


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
        ConfigPropertyDefinition(property_key="a", required=False, property_type=int),
        ConfigPropertyDefinition(property_key="b", required=False, property_type=float),
        ConfigPropertyDefinition(
            property_key="complex",
            required=False,
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
            required=False,
            property_type=None,
            nested_properties=[
                ConfigPropertyDefinition(
                    property_key="nested_subclass",
                    required=False,
                    property_type=None,
                    nested_properties=[
                        ConfigPropertyDefinition(property_key="some_property", required=False, property_type=bool)
                    ],
                ),
                ConfigPropertyDefinition(property_key="other_property", required=False, property_type=float),
                ConfigPropertyDefinition(property_key="uuid", required=False, property_type=UUID),
            ],
        ),
    )
