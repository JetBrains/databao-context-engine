import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, TypedDict

from pydantic import BaseModel

from databao_context_engine.serialization.yaml import to_yaml_string, write_yaml_to_stream


class PydanticClass(BaseModel):
    my_str: str = "123"
    my_date: date


class CustomClass:
    def __init__(self):
        self._hidden_var = "_hidden_var"
        self.exposed_var = "exposed_var"
        self.my_list = ["1", "2", "3"]


class CustomClassNoPublicFields:
    def __init__(self):
        self._hidden_var = "Beautiful"
        self._second_hidden_var = "Content"

    def __str__(self):
        return f"{self._hidden_var} {self._second_hidden_var}"


@dataclass
class SimpleNestedClass:
    nested_var: str


@dataclass
class Dataclass:
    my_str: str
    my_nested_class: SimpleNestedClass
    my_int: int = 12
    my_uuid: uuid.UUID = uuid.uuid4()
    my_date: datetime = datetime.now()


class TypedDictionary(TypedDict):
    my_var: float


def get_input(my_uuid: uuid.UUID, my_date: datetime) -> Any:
    return {
        "dataclass": Dataclass("hello", my_uuid=my_uuid, my_date=my_date, my_nested_class=SimpleNestedClass("nested")),
        "pydantic": PydanticClass(my_date=date(2025, 1, 1)),
        "custom": CustomClass(),
        "tuple": (1, "text"),
        "list": [TypedDictionary(my_var=1.0), TypedDictionary(my_var=2.0), TypedDictionary(my_var=3.0)],
    }


def get_expected(my_uuid, now):
    return f"""
dataclass:
  my_str: hello
  my_nested_class:
    nested_var: nested
  my_int: 12
  my_uuid: {str(my_uuid)}
  my_date: {now.isoformat(" ")}
pydantic:
  my_str: '123'
  my_date: 2025-01-01
custom:
  exposed_var: exposed_var
  my_list:
  - '1'
  - '2'
  - '3'
tuple:
- 1
- text
list:
- my_var: 1.0
- my_var: 2.0
- my_var: 3.0
        """


def test_to_yaml_string():
    my_uuid = uuid.uuid4()
    now = datetime.now()
    result = to_yaml_string(get_input(my_uuid, now))

    assert result.strip() == get_expected(my_uuid, now).strip()


def test_write_yaml_to_file(tmp_path: Path):
    my_uuid = uuid.uuid4()
    now = datetime.now()

    test_file = tmp_path / "test_write_yaml_to_file.yaml"
    with open(test_file, "w") as f:
        write_yaml_to_stream(data=get_input(my_uuid, now), file_stream=f)

    result = test_file.read_text()

    assert result.strip() == get_expected(my_uuid, now).strip()


def test_default_dict():
    d = defaultdict()
    d["a"] = 1
    d["b"] = 2
    result = to_yaml_string(d)

    assert result.strip() == "a: 1\nb: 2"


def test_object_with_no_public_field():
    result = to_yaml_string({"my_attribute": CustomClassNoPublicFields()})

    assert result.strip() == "my_attribute: Beautiful Content"
