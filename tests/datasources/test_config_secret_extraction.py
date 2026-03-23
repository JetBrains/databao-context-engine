from dataclasses import dataclass, field
from typing import Any

import databao_context_engine.datasources.config_secret_extraction as config_secret_extraction_module
from databao_context_engine.datasources.config_secret_extraction import (
    build_secret_key_prefix,
    extract_secrets_from_config,
)


@dataclass
class FakeConfigPropertyDefinition:
    property_key: str
    secret: bool = False
    nested_properties: list[Any] = field(default_factory=list)


@dataclass
class FakeConfigUnionPropertyDefinition(FakeConfigPropertyDefinition):
    type_properties: dict[str, list[Any]] = field(default_factory=dict)
    default_type: str | None = None


def as_properties(*properties: Any) -> list[Any]:
    return list(properties)


def test_build_secret_key_prefix_uses_relative_path_without_extension() -> None:
    assert build_secret_key_prefix("databases/my_pg.yaml") == "databases.my_pg"


def test_extract_secrets_from_config_replaces_flat_secret_field() -> None:
    config = {"password": "secret123"}
    properties = as_properties(
        FakeConfigPropertyDefinition("password", secret=True),
    )

    result = extract_secrets_from_config(
        config_content=config,
        properties=properties,
        datasource_relative_path="databases/my_pg.yaml",
    )

    assert result.config_with_secret_refs == {
        "password": "${secret:databases.my_pg.password}",
    }
    assert result.secrets == {
        "databases.my_pg.password": "secret123",
    }
    assert config == {"password": "secret123"}


def test_extract_secrets_from_config_replaces_nested_secret_field() -> None:
    config = {
        "connection": {
            "host": "localhost",
            "password": "secret123",
        }
    }
    properties = as_properties(
        FakeConfigPropertyDefinition(
            "connection",
            nested_properties=[
                FakeConfigPropertyDefinition("host"),
                FakeConfigPropertyDefinition("password", secret=True),
            ],
        )
    )

    result = extract_secrets_from_config(
        config_content=config,
        properties=properties,
        datasource_relative_path="databases/my_pg.yaml",
    )

    assert result.config_with_secret_refs == {
        "connection": {
            "host": "localhost",
            "password": "${secret:databases.my_pg.connection.password}",
        }
    }
    assert result.secrets == {
        "databases.my_pg.connection.password": "secret123",
    }


def test_extract_secrets_from_config_ignores_non_secret_fields() -> None:
    config = {
        "connection": {
            "host": "localhost",
        }
    }
    properties = as_properties(
        FakeConfigPropertyDefinition(
            "connection",
            nested_properties=[
                FakeConfigPropertyDefinition("host"),
            ],
        )
    )

    result = extract_secrets_from_config(
        config_content=config,
        properties=properties,
        datasource_relative_path="databases/my_pg.yaml",
    )

    assert result.config_with_secret_refs == config
    assert result.secrets == {}


def test_extract_secrets_from_config_keeps_existing_secret_reference() -> None:
    config = {
        "connection": {
            "password": "${secret:databases.my_pg.connection.password}",
        }
    }
    properties = as_properties(
        FakeConfigPropertyDefinition(
            "connection",
            nested_properties=[
                FakeConfigPropertyDefinition("password", secret=True),
            ],
        )
    )

    result = extract_secrets_from_config(
        config_content=config,
        properties=properties,
        datasource_relative_path="databases/my_pg.yaml",
    )

    assert result.config_with_secret_refs == config
    assert result.secrets == {}


def test_extract_secrets_from_config_keeps_existing_env_var_reference() -> None:
    config = {
        "connection": {
            "password": "{{ env_var('PG_PASSWORD') }}",
        }
    }
    properties = as_properties(
        FakeConfigPropertyDefinition(
            "connection",
            nested_properties=[
                FakeConfigPropertyDefinition("password", secret=True),
            ],
        )
    )

    result = extract_secrets_from_config(
        config_content=config,
        properties=properties,
        datasource_relative_path="databases/my_pg.yaml",
    )

    assert result.config_with_secret_refs == config
    assert result.secrets == {}


def test_extract_secrets_from_config_skips_none_secret_values() -> None:
    config = {
        "connection": {
            "password": None,
        }
    }
    properties = as_properties(
        FakeConfigPropertyDefinition(
            "connection",
            nested_properties=[
                FakeConfigPropertyDefinition("password", secret=True),
            ],
        )
    )

    result = extract_secrets_from_config(
        config_content=config,
        properties=properties,
        datasource_relative_path="databases/my_pg.yaml",
    )

    assert result.config_with_secret_refs == config
    assert result.secrets == {}


def test_extract_secrets_from_config_uses_union_type_to_find_secret_field(monkeypatch) -> None:
    monkeypatch.setattr(
        config_secret_extraction_module,
        "ConfigUnionPropertyDefinition",
        FakeConfigUnionPropertyDefinition,
    )
    config = {
        "auth": {
            "type": "key_pair",
            "private_key": "secret123",
        }
    }
    properties = as_properties(
        FakeConfigUnionPropertyDefinition(
            property_key="auth",
            type_properties={
                "password": [
                    FakeConfigPropertyDefinition("type"),
                    FakeConfigPropertyDefinition("password", secret=True),
                ],
                "key_pair": [
                    FakeConfigPropertyDefinition("type"),
                    FakeConfigPropertyDefinition("private_key", secret=True),
                ],
            },
        )
    )

    result = extract_secrets_from_config(
        config_content=config,
        properties=properties,
        datasource_relative_path="warehouse/prod.yaml",
    )

    assert result.config_with_secret_refs == {
        "auth": {
            "type": "key_pair",
            "private_key": "${secret:warehouse.prod.auth.private_key}",
        }
    }
    assert result.secrets == {
        "warehouse.prod.auth.private_key": "secret123",
    }


def test_extract_secrets_from_config_uses_default_union_branch_when_type_is_missing(monkeypatch) -> None:
    monkeypatch.setattr(
        config_secret_extraction_module,
        "ConfigUnionPropertyDefinition",
        FakeConfigUnionPropertyDefinition,
    )
    config = {
        "auth": {
            "password": "secret123",
        }
    }
    properties = as_properties(
        FakeConfigUnionPropertyDefinition(
            property_key="auth",
            default_type="password",
            type_properties={
                "password": [
                    FakeConfigPropertyDefinition("password", secret=True),
                ],
            },
        )
    )

    result = extract_secrets_from_config(
        config_content=config,
        properties=properties,
        datasource_relative_path="warehouse/prod.yaml",
    )

    assert result.config_with_secret_refs == {
        "auth": {
            "password": "${secret:warehouse.prod.auth.password}",
        }
    }
    assert result.secrets == {
        "warehouse.prod.auth.password": "secret123",
    }
