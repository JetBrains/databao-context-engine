from contextlib import nullcontext
from pathlib import Path

import pytest

from databao_context_engine.datasources.types import DatasourceId


@pytest.mark.parametrize(
    ["config_folder", "datasource_name", "suffix", "is_valid", "expected_error_message"],
    [
        ("parent/child", "my_datasource", ".yaml", False, "must not contain a path separator"),
        ("parent/", "my_datasource", ".yaml", False, "must not contain a path separator"),
        ("parent", "child/my_datasource", ".yaml", False, "must not contain a path separator"),
        ("parent", "my_datasource.yaml", ".yaml", False, "not contain the file suffix"),
        ("parent", "my_datasource", "yaml", False, "config_file_suffix must start with a dot"),
        ("parent", "my_datasource", ".yaml", True, ""),
        ("parent", "my_datasource.txt", ".yaml", True, ""),
        ("Case Sensitive Folder", "My Datasource", ".yaml", True, ""),
    ],
)
def test_datasource_id__fails_to_create_invalid_id(
    config_folder: str, datasource_name: str, suffix: str, is_valid: bool, expected_error_message: str
):
    context_manager = nullcontext() if is_valid else pytest.raises(ValueError)
    with context_manager as e:
        DatasourceId(
            datasource_config_folder=config_folder,
            datasource_name=datasource_name,
            config_file_suffix=suffix,
        )

    if not is_valid:
        # Required for Mypy
        assert e is not None
        assert expected_error_message in str(e.value)


@pytest.mark.parametrize(
    ["string_repr", "is_valid", "expected_error_message", "expected_datasource_id"],
    [
        ("parent/child/my_datasource.yaml", False, "too many parent folders", None),
        ("parent/my_datasource", False, "must not be empty", None),
        ("parent/", False, "must not be empty", None),
        ("my_datasource", False, "must not be empty", None),
        (
            "parent/my_datasource.yaml",
            True,
            "",
            DatasourceId(
                datasource_config_folder="parent", datasource_name="my_datasource", config_file_suffix=".yaml"
            ),
        ),
        (
            "parent//my_datasource.yaml",
            True,
            "",
            DatasourceId(
                datasource_config_folder="parent", datasource_name="my_datasource", config_file_suffix=".yaml"
            ),
        ),
        (
            "parent/my_datasource.txt.yaml",
            True,
            "",
            DatasourceId(
                datasource_config_folder="parent", datasource_name="my_datasource.txt", config_file_suffix=".yaml"
            ),
        ),
        (
            "Case Sensitive Folder/My Datasource.yaml",
            True,
            "",
            DatasourceId(
                datasource_config_folder="Case Sensitive Folder",
                datasource_name="My Datasource",
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
    ["config_file_path", "is_valid", "expected_error_message", "expected_datasource_id"],
    [
        (Path("parent/my_datasource"), False, "must not be empty", None),
        (Path("parent/"), False, "must not be empty", None),
        (Path("my_datasource"), False, "must not be empty", None),
        (
            Path("my-dce-project/src/databases/my_datasource.yaml"),
            True,
            "",
            DatasourceId(
                datasource_config_folder="databases", datasource_name="my_datasource", config_file_suffix=".yaml"
            ),
        ),
        (
            Path("parent/my_datasource.yaml"),
            True,
            "",
            DatasourceId(
                datasource_config_folder="parent", datasource_name="my_datasource", config_file_suffix=".yaml"
            ),
        ),
        (
            Path("parent/my_datasource.txt.yaml"),
            True,
            "",
            DatasourceId(
                datasource_config_folder="parent", datasource_name="my_datasource.txt", config_file_suffix=".yaml"
            ),
        ),
        (
            Path("Case Sensitive Folder/My Datasource.yaml"),
            True,
            "",
            DatasourceId(
                datasource_config_folder="Case Sensitive Folder",
                datasource_name="My Datasource",
                config_file_suffix=".yaml",
            ),
        ),
    ],
)
def test_datasource_id__from_config_file_path(
    config_file_path: Path, is_valid: bool, expected_error_message: str, expected_datasource_id: DatasourceId | None
):
    context_manager = nullcontext() if is_valid else pytest.raises(ValueError)
    with context_manager as e:
        result = DatasourceId.from_datasource_config_file_path(config_file_path)
        if is_valid:
            assert result is not None
            assert result == expected_datasource_id

    if not is_valid:
        # Required for Mypy
        assert e is not None
        assert expected_error_message in str(e.value)


def test_datasource_id__relative_path_to_config_file_from_yaml_file():
    under_test = DatasourceId(
        datasource_config_folder="parent", datasource_name="my_datasource", config_file_suffix=".yaml"
    )

    result = under_test.relative_path_to_config_file()

    assert result == Path("parent/my_datasource.yaml")


def test_datasource_id__relative_path_to_config_file_from_raw_file():
    under_test = DatasourceId(
        datasource_config_folder="parent", datasource_name="my_datasource", config_file_suffix=".txt"
    )

    result = under_test.relative_path_to_config_file()

    assert result == Path("parent/my_datasource.txt")


def test_datasource_id__relative_path_to_context_file_from_yaml_file():
    under_test = DatasourceId(
        datasource_config_folder="parent", datasource_name="my_datasource", config_file_suffix=".yaml"
    )

    result = under_test.relative_path_to_context_file()

    assert result == Path("parent/my_datasource.yaml")


def test_datasource_id__relative_path_to_context_file_from_raw_file():
    under_test = DatasourceId(
        datasource_config_folder="parent", datasource_name="my_datasource", config_file_suffix=".txt"
    )

    result = under_test.relative_path_to_context_file()

    assert result == Path("parent/my_datasource.txt.yaml")


def test_datasource_id__serialize_and_deserialize():
    input = "parent/my_datasource.yaml"

    deserialized = DatasourceId.from_string_repr("parent/my_datasource.yaml")

    assert str(deserialized) == input
    assert DatasourceId.from_string_repr(str(deserialized)) == deserialized
