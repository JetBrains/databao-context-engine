from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.plugins.databases.postgresql.config_file import PostgresConfigFile
from databao_context_engine.plugins.databases.snowflake.config_file import (
    SnowflakeConfigFile,
    SnowflakeKeyPairAuth,
    SnowflakeOAuthAuth,
    SnowflakePasswordAuth,
)
from databao_context_engine.plugins.databases.sqlite.config_file import SQLiteConfigFile
from databao_context_engine.plugins.databases.sqlite.sqlite_db_plugin import SQLiteDbPlugin

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "build_dbt_database_context.py"


@pytest.fixture(scope="module")
def module() -> Any:
    spec = importlib.util.spec_from_file_location("build_dbt_database_context", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise AssertionError("Unable to load build_dbt_database_context.py")
    loaded_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded_module)
    return loaded_module


def test_build_postgres_config(module: Any) -> None:
    credentials = SimpleNamespace(
        type="postgres",
        host="localhost",
        port=5432,
        dbname="warehouse",
        user="dbt_user",
        password="secret",
        role="analytics_role",
        sslmode="require",
        sslcert="/tmp/client.crt",
        sslkey="/tmp/client.key",
        sslrootcert="/tmp/root.crt",
    )

    result = module.build_postgres_config(credentials, "warehouse_source", DatasourceType(full_type="postgres"))

    assert isinstance(result, PostgresConfigFile)
    assert result.name == "warehouse_source"
    assert result.type == "postgres"
    assert result.connection.host == "localhost"
    assert result.connection.port == 5432
    assert result.connection.database == "warehouse"
    assert result.connection.user == "dbt_user"
    assert result.connection.password == "secret"
    assert result.connection.additional_properties == {
        "server_settings": {"role": "analytics_role"},
        "sslmode": "require",
        "sslcert": "/tmp/client.crt",
        "sslkey": "/tmp/client.key",
        "sslrootcert": "/tmp/root.crt",
    }


def test_build_snowflake_config__password_auth(module: Any) -> None:
    credentials = SimpleNamespace(
        type="snowflake",
        account="my-account",
        warehouse="transforming",
        database="analytics",
        user="dbt_user",
        role="transformer",
        password="secret",
        host="snowflake.local",
        port=443,
        protocol="https",
        proxy_host="proxy.local",
        proxy_port=8443,
        insecure_mode=True,
        connect_timeout=30,
        connect_retries=2,
        query_tag="ignored",
    )

    result = module.build_snowflake_config(credentials, "snowflake_source", DatasourceType(full_type="snowflake"))

    assert isinstance(result, SnowflakeConfigFile)
    assert result.name == "snowflake_source"
    assert result.type == "snowflake"
    assert result.connection.account == "my-account"
    assert result.connection.warehouse == "transforming"
    assert result.connection.database == "analytics"
    assert result.connection.user == "dbt_user"
    assert result.connection.role == "transformer"
    assert isinstance(result.connection.auth, SnowflakePasswordAuth)
    assert result.connection.auth.password == "secret"
    assert result.connection.additional_properties == {
        "host": "snowflake.local",
        "port": 443,
        "protocol": "https",
        "proxy_host": "proxy.local",
        "proxy_port": 8443,
        "insecure_mode": True,
    }


def test_build_snowflake_config__key_pair_auth(module: Any) -> None:
    credentials = SimpleNamespace(
        type="snowflake",
        account="my-account",
        warehouse="transforming",
        database="analytics",
        user="dbt_user",
        role="transformer",
        private_key_path="/tmp/private_key.p8",
        private_key_passphrase="passphrase",
    )

    result = module.build_snowflake_config(credentials, "snowflake_source", DatasourceType(full_type="snowflake"))

    assert isinstance(result.connection.auth, SnowflakeKeyPairAuth)
    assert result.connection.auth.private_key is None
    assert result.connection.auth.private_key_file == "/tmp/private_key.p8"
    assert result.connection.auth.private_key_file_pwd == "passphrase"


def test_build_snowflake_config__token_auth(module: Any) -> None:
    credentials = SimpleNamespace(
        type="snowflake",
        account="my-account",
        warehouse="transforming",
        database="analytics",
        user="dbt_user",
        role="transformer",
        token="token-value",
    )

    result = module.build_snowflake_config(credentials, "snowflake_source", DatasourceType(full_type="snowflake"))

    assert isinstance(result.connection.auth, SnowflakeOAuthAuth)
    assert result.connection.auth.token == "token-value"


def test_build_sqlite_config(module: Any) -> None:
    credentials = SimpleNamespace(
        type="sqlite",
        schema="main",
        schemas_and_paths={"main": "/tmp/example.sqlite"},
    )

    result = module.build_sqlite_config(credentials, "sqlite_source", DatasourceType(full_type="sqlite"))

    assert isinstance(result, SQLiteConfigFile)
    assert result.name == "sqlite_source"
    assert result.type == "sqlite"
    assert result.connection.database_path == "/tmp/example.sqlite"


def test_extract_and_resolve_adapter_type__uses_explicit_mapping(module: Any) -> None:
    credentials = SimpleNamespace(type="postgres")

    adapter_type = module.extract_dbt_adapter_type(credentials)
    datasource_type = module.resolve_supported_datasource_type(adapter_type)

    assert adapter_type == "postgres"
    assert datasource_type == DatasourceType(full_type="postgres")


def test_resolve_supported_datasource_type__fails_for_unsupported_adapter(module: Any) -> None:
    with pytest.raises(ValueError, match="Unsupported dbt adapter type 'bigquery'"):
        module.resolve_supported_datasource_type("bigquery")


def test_build_typed_datasource_config__fails_for_missing_adapter_type(module: Any) -> None:
    credentials = SimpleNamespace()

    with pytest.raises(ValueError, match="Resolved dbt credentials do not expose a valid adapter type"):
        module.build_typed_datasource_config(credentials, "missing_type_source")


def test_map_snowflake_auth__default_authenticator_without_password_fails(module: Any) -> None:
    credentials = SimpleNamespace(
        type="snowflake",
        account="my-account",
        authenticator="snowflake",
        user="dbt_user",
    )

    with pytest.raises(
        ValueError,
        match="Snowflake credentials are using the default password authenticator, but no password was provided.",
    ):
        module.map_snowflake_auth(credentials)


def test_resolve_sqlite_database_path__fails_without_matching_schema_or_main(module: Any) -> None:
    credentials = SimpleNamespace(
        type="sqlite",
        schema="analytics",
        schemas_and_paths={"staging": "/tmp/staging.sqlite"},
    )

    with pytest.raises(ValueError, match="The first available path would have been: /tmp/staging.sqlite"):
        module.resolve_sqlite_database_path(credentials)


def test_build_context_from_profile__executes_plugin_directly(module: Any, mocker) -> None:

    class FakePlugin(SQLiteDbPlugin):
        def build_context(self, full_type: str, datasource_name: str, file_config: SQLiteConfigFile) -> dict[str, Any]:
            return {
                "full_type": full_type,
                "datasource_name": datasource_name,
                "database_path": file_config.connection.database_path,
            }

    class FakeLoader:
        def __init__(self) -> None:
            self.requested_type: DatasourceType | None = None

        def get_plugin_for_datasource_type(self, datasource_type: DatasourceType) -> FakePlugin:
            self.requested_type = datasource_type
            return FakePlugin()

    profile = SimpleNamespace(
        credentials=SimpleNamespace(type="sqlite", schema="main", schemas_and_paths={"main": "/tmp/demo.sqlite"})
    )
    fake_loader = FakeLoader()

    mocker.patch.object(module, "load_dbt_profile", return_value=profile)

    built_context = module.build_context_from_dbt_project(
        project_path=Path("/tmp/fake_dbt_project"),
        datasource_name="demo_source",
        plugin_loader=fake_loader,
    )
    document = yaml.safe_load(module.to_yaml_string(built_context))

    assert fake_loader.requested_type == DatasourceType(full_type="sqlite")
    assert document["datasource_id"] == "demo_source.yaml"
    assert document["datasource_type"] == "sqlite"
    assert document["context"] == {
        "full_type": "sqlite",
        "datasource_name": "demo_source",
        "database_path": "/tmp/demo.sqlite",
    }
