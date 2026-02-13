from contextlib import nullcontext
from pathlib import Path

import pytest

from databao_context_engine.datasources.types import DatasourceId


@pytest.mark.parametrize(
    ["datasource_path", "suffix", "is_valid", "expected_error_message"],
    [
        ("parent/child/my_datasource", ".yaml", True, ""),
        ("parent/my_datasource.yaml", ".yaml", False, "not contain the file suffix"),
        ("parent/my_datasource", "yaml", False, "config_file_suffix must start with a dot"),
        ("parent/my_datasource", ".yaml", True, ""),
        ("parent/my_datasource.txt", ".yaml", True, ""),
        ("Case Sensitive Folder/My Datasource", ".yaml", True, ""),
    ],
)
def test_datasource_id__fails_to_create_invalid_id(
    datasource_path: str, suffix: str, is_valid: bool, expected_error_message: str
):
    context_manager = nullcontext() if is_valid else pytest.raises(ValueError)
    with context_manager as e:
        DatasourceId(
            datasource_path=datasource_path,
            config_file_suffix=suffix,
        )

    if not is_valid:
        # Required for Mypy
        assert e is not None
        assert expected_error_message in str(e.value)


@pytest.mark.parametrize(
    ["string_repr", "is_valid", "expected_error_message", "expected_datasource_id"],
    [
        (
            "parent/child/my_datasource.yaml",
            True,
            "",
            DatasourceId(datasource_path="parent/child/my_datasource", config_file_suffix=".yaml"),
        ),
        ("parent/my_datasource", False, "must not be empty", None),
        ("parent/", False, "must not be empty", None),
        ("my_datasource", False, "must not be empty", None),
        (
            "parent/my_datasource.yaml",
            True,
            "",
            DatasourceId(datasource_path="parent/my_datasource", config_file_suffix=".yaml"),
        ),
        (
            "parent//my_datasource.yaml",
            True,
            "",
            DatasourceId(datasource_path="parent/my_datasource", config_file_suffix=".yaml"),
        ),
        (
            "parent/my_datasource.txt.yaml",
            True,
            "",
            DatasourceId(datasource_path="parent/my_datasource.txt", config_file_suffix=".yaml"),
        ),
        (
            "parent1/parent2/my_datasource.txt.yaml",
            True,
            "",
            DatasourceId(datasource_path="parent1/parent2/my_datasource.txt", config_file_suffix=".yaml"),
        ),
        (
            "Case Sensitive Folder/My Datasource.yaml",
            True,
            "",
            DatasourceId(
                datasource_path="Case Sensitive Folder/My Datasource",
                config_file_suffix=".yaml",
            ),
        ),
    ],
)
def test_datasource_id__from_string_repr(
    string_repr: str, is_valid: bool, expected_error_message: str, expected_datasource_id: DatasourceId | None
):
    context_manager = nullcontext() if is_valid else pytest.raises(ValueError)
    with context_manager as e:
        result = DatasourceId.from_string_repr(string_repr)
        if is_valid:
            assert result is not None
            assert result == expected_datasource_id

    if not is_valid:
        # Required for Mypy
        assert e is not None
        assert expected_error_message in str(e.value)


@pytest.mark.parametrize(
    ["config_file", "is_valid", "expected_error_message", "expected_datasource_id"],
    [
        ("parent/my_datasource", False, "must not be empty", None),
        ("parent/", False, "must not be empty", None),
        ("my_datasource", False, "must not be empty", None),
        (
            "parent/my_datasource.yaml",
            True,
            "",
            DatasourceId(datasource_path="parent/my_datasource", config_file_suffix=".yaml"),
        ),
        (
            "parent/my_datasource.txt.yaml",
            True,
            "",
            DatasourceId(datasource_path="parent/my_datasource.txt", config_file_suffix=".yaml"),
        ),
        (
            "Case Sensitive Folder/My Datasource.yaml",
            True,
            "",
            DatasourceId(
                datasource_path="Case Sensitive Folder/My Datasource",
                config_file_suffix=".yaml",
            ),
        ),
    ],
)
def test_datasource_id__from_config_file_path(
    config_file: str, is_valid: bool, expected_error_message: str, expected_datasource_id: DatasourceId | None
):
    context_manager = nullcontext() if is_valid else pytest.raises(ValueError)
    with context_manager as e:
        result = DatasourceId.from_string_repr(config_file)
        if is_valid:
            assert result is not None
            assert result == expected_datasource_id

    if not is_valid:
        # Required for Mypy
        assert e is not None
        assert expected_error_message in str(e.value)


def test_datasource_id__relative_path_to_config_file_from_yaml_file():
    under_test = DatasourceId(datasource_path="parent/my_datasource", config_file_suffix=".yaml")

    result = under_test.relative_path_to_config_file()

    assert result == Path("parent/my_datasource.yaml")


def test_datasource_id__relative_path_to_config_file_from_raw_file():
    under_test = DatasourceId(datasource_path="parent/my_datasource", config_file_suffix=".txt")

    result = under_test.relative_path_to_config_file()

    assert result == Path("parent/my_datasource.txt")


def test_datasource_id__relative_path_to_context_file_from_yaml_file():
    under_test = DatasourceId(datasource_path="parent/my_datasource", config_file_suffix=".yaml")

    result = under_test.relative_path_to_context_file()

    assert result == Path("parent/my_datasource.yaml")


def test_datasource_id__relative_path_to_context_file_from_raw_file():
    under_test = DatasourceId(datasource_path="parent/my_datasource", config_file_suffix=".txt")

    result = under_test.relative_path_to_context_file()

    assert result == Path("parent/my_datasource.txt.yaml")


def test_datasource_id__serialize_and_deserialize():
    input = "parent/my_datasource.yaml"

    deserialized = DatasourceId.from_string_repr(input)

    assert str(deserialized) == input
    assert DatasourceId.from_string_repr(str(deserialized)) == deserialized


@pytest.mark.parametrize(
    ["input_datasource_id", "expected_datasource_name"],
    [
        ("my_datasource.yaml", "my_datasource"),
        ("parent/my_datasource.yaml", "my_datasource"),
        ("parent/my_datasource.txt", "my_datasource.txt"),
        ("files/my_datasource.yaml", "my_datasource.yaml"),
    ],
)
def test_datasource_id__datasource_name(input_datasource_id: str, expected_datasource_name: str):
    datasource_id = DatasourceId.from_string_repr(input_datasource_id)

    assert datasource_id.name == expected_datasource_name
