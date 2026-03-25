import uuid
from pathlib import Path

import pytest

from databao_context_engine.llm.config import EmbeddingModelDetails
from databao_context_engine.project.layout import ProjectLayout
from databao_context_engine.project.project_config import ProjectConfig
from databao_context_engine.project.project_secrets import (
    load_project_secrets,
    load_secrets_file,
    make_secret_ref,
    merge_and_store_project_secrets,
    parse_secret_ref,
    resolve_references,
)


def make_project_layout(project_dir: Path) -> ProjectLayout:
    return ProjectLayout(
        project_dir=project_dir,
        project_config=ProjectConfig(
            project_id=uuid.uuid4(),
            ollama_embedding_model_details=EmbeddingModelDetails.default(),
        ),
    )


def test_make_secret_ref() -> None:
    assert make_secret_ref("databases.my_pg.connection.password") == ("${secret:databases.my_pg.connection.password}")


def test_parse_secret_ref_returns_key_for_valid_ref() -> None:
    assert parse_secret_ref("${secret:my.secret}") == "my.secret"


def test_parse_secret_ref_returns_none_for_plain_string() -> None:
    assert parse_secret_ref("plain-value") is None


def test_load_secrets_file_returns_empty_dict_when_file_does_not_exist(tmp_path: Path) -> None:
    file_path = tmp_path / ".secrets.yaml"

    assert load_secrets_file(file_path) == {}


def test_load_secrets_file_returns_empty_dict_for_empty_file(tmp_path: Path) -> None:
    file_path = tmp_path / ".secrets.yaml"
    file_path.write_text("")

    assert load_secrets_file(file_path) == {}


def test_load_secrets_file_loads_yaml_mapping(tmp_path: Path) -> None:
    file_path = tmp_path / ".secrets.yaml"
    file_path.write_text(
        """
db.password: secret123
api.token: abc
""".strip()
    )

    assert load_secrets_file(file_path) == {
        "db.password": "secret123",
        "api.token": "abc",
    }


def test_load_secrets_file_raises_for_non_mapping_yaml(tmp_path: Path) -> None:
    file_path = tmp_path / ".secrets.yaml"
    file_path.write_text("- one\n- two\n")

    with pytest.raises(ValueError, match="Secrets file must contain a mapping"):
        load_secrets_file(file_path)


def test_merge_and_store_project_secrets_creates_file_and_persists_values(tmp_path: Path) -> None:
    project_layout = make_project_layout(tmp_path)

    merge_and_store_project_secrets(
        project_layout,
        {"db.password": "secret123"},
    )

    assert load_project_secrets(project_layout) == {
        "db.password": "secret123",
    }


def test_merge_and_store_project_secrets_merges_with_existing_values(tmp_path: Path) -> None:
    project_layout = make_project_layout(tmp_path)

    merge_and_store_project_secrets(
        project_layout,
        {"db.password": "secret123"},
    )
    merge_and_store_project_secrets(
        project_layout,
        {"api.token": "abc"},
    )

    assert load_project_secrets(project_layout) == {
        "db.password": "secret123",
        "api.token": "abc",
    }


def test_merge_and_store_project_secrets_overwrites_existing_key(tmp_path: Path) -> None:
    project_layout = make_project_layout(tmp_path)

    merge_and_store_project_secrets(
        project_layout,
        {"db.password": "old-value"},
    )
    merge_and_store_project_secrets(
        project_layout,
        {"db.password": "new-value"},
    )

    assert load_project_secrets(project_layout) == {
        "db.password": "new-value",
    }


def test_resolve_secret_references_replaces_secret_refs_in_nested_data() -> None:
    value = {
        "connection": {
            "password": "${secret:db.password}",
            "options": ["keep-me", "${secret:api.token}"],
        }
    }
    secrets = {
        "db.password": "secret123",
        "api.token": "abc",
    }

    resolved = resolve_references(value, secrets)

    assert resolved == {
        "connection": {
            "password": "secret123",
            "options": ["keep-me", "abc"],
        }
    }


def test_resolve_secret_references_leaves_plain_strings_unchanged() -> None:
    value = {
        "connection": {
            "host": "localhost",
            "user": "admin",
        }
    }

    resolved = resolve_references(value, {"unused": "value"})

    assert resolved == value


def test_resolve_secret_references_raises_for_missing_secret() -> None:
    value = {
        "connection": {
            "password": "${secret:db.password}",
        }
    }

    with pytest.raises(ValueError, match="db.password"):
        resolve_references(value, {})
