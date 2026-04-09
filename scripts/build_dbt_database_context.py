from __future__ import annotations

import argparse
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dbt.config.runtime import load_profile
from dbt.flags import set_from_args

from databao_context_engine.build_sources.plugin_execution import BuiltDatasourceContext
from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.pluginlib.plugin_utils import execute_datasource_plugin
from databao_context_engine.plugins.databases.base_db_plugin import BaseDatabasePlugin
from databao_context_engine.plugins.databases.postgresql.config_file import (
    PostgresConfigFile,
    PostgresConnectionProperties,
)
from databao_context_engine.plugins.databases.snowflake.config_file import (
    SnowflakeConfigFile,
    SnowflakeConnectionProperties,
    SnowflakeKeyPairAuth,
    SnowflakeOAuthAuth,
    SnowflakePasswordAuth,
    SnowflakeSSOAuth,
)
from databao_context_engine.plugins.databases.sqlite.config_file import SQLiteConfigFile, SQLiteConnectionConfig
from databao_context_engine.plugins.plugin_loader import DatabaoContextPluginLoader
from databao_context_engine.serialization.yaml import to_yaml_string

ADAPTER_TO_DATASOURCE_TYPE = {
    "postgres": DatasourceType(full_type="postgres"),
    "snowflake": DatasourceType(full_type="snowflake"),
    "sqlite": DatasourceType(full_type="sqlite"),
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a Databao database context directly from a dbt project's resolved target profile."
    )
    parser.add_argument("project_path", type=Path, help="Path to the dbt project directory")
    parser.add_argument(
        "--datasource-name",
        dest="datasource_name",
        help="Override the generated datasource name. Defaults to the dbt project directory name.",
    )
    return parser.parse_args(argv)


def validate_project_path(project_path: Path) -> Path:
    resolved_path = project_path.expanduser().resolve()

    if not resolved_path.exists():
        raise ValueError(f"dbt project path does not exist: {resolved_path}")
    if not resolved_path.is_dir():
        raise ValueError(f"dbt project path is not a directory: {resolved_path}")
    if not resolved_path.joinpath("dbt_project.yml").is_file():
        raise ValueError(f"dbt project path does not contain dbt_project.yml: {resolved_path}")

    return resolved_path


def load_dbt_profile(project_path: Path) -> Any:
    set_from_args(
        argparse.Namespace(
            PROFILES_DIR=str(project_path),
            PROJECT_DIR=str(project_path),
            profile=None,
            target=None,
            threads=None,
        ),
        None,
    )
    return load_profile(project_root=str(project_path), cli_vars={})


def resolve_supported_datasource_type(adapter_type: str) -> DatasourceType:
    datasource_type = ADAPTER_TO_DATASOURCE_TYPE.get(adapter_type)
    if datasource_type is None:
        supported = ", ".join(ADAPTER_TO_DATASOURCE_TYPE)
        raise ValueError(f"Unsupported dbt adapter type '{adapter_type}'. Supported types: {supported}")
    return datasource_type


def build_datasource_config_from_profile(
    profile: Any, datasource_name: str
) -> PostgresConfigFile | SnowflakeConfigFile | SQLiteConfigFile:
    credentials = getattr(profile, "credentials", None)
    if credentials is None:
        raise ValueError("Resolved dbt profile does not expose credentials")

    return build_typed_datasource_config(credentials, datasource_name)


def build_typed_datasource_config(
    credentials: Any,
    datasource_name: str,
) -> PostgresConfigFile | SnowflakeConfigFile | SQLiteConfigFile:
    adapter_type = extract_dbt_adapter_type(credentials)
    if not isinstance(adapter_type, str) or not adapter_type:
        raise ValueError("Resolved dbt credentials do not expose a valid adapter type")

    datasource_type = resolve_supported_datasource_type(adapter_type)

    if datasource_type == DatasourceType(full_type="postgres"):
        return build_postgres_config(credentials, datasource_name, datasource_type)
    if datasource_type == DatasourceType(full_type="snowflake"):
        return build_snowflake_config(credentials, datasource_name, datasource_type)
    if datasource_type == DatasourceType(full_type="sqlite"):
        return build_sqlite_config(credentials, datasource_name, datasource_type)
    raise ValueError(f"Unsupported dbt adapter type '{adapter_type}'")


def build_postgres_config(
    credentials: Any, datasource_name: str, datasource_type: DatasourceType
) -> PostgresConfigFile:
    return PostgresConfigFile(
        name=datasource_name,
        type=datasource_type.full_type,
        connection=PostgresConnectionProperties(
            host=require_attr(credentials, "host"),
            port=optional_attr(credentials, "port"),
            database=optional_attr(credentials, "dbname", "database"),
            user=optional_attr(credentials, "user"),
            password=optional_attr(credentials, "password", "pass"),
            additional_properties=map_postgres_additional_properties(credentials),
        ),
    )


def _get_additional_property(credentials: Any, credentials_key: str, additional_properties_key: str) -> dict[str, Any]:
    credentials_attr_value = optional_attr(credentials, credentials_key)
    if credentials_attr_value is None:
        return {}

    return {additional_properties_key: credentials_attr_value}


def map_postgres_additional_properties(credentials: Any) -> dict[str, Any]:
    additional_properties: dict[str, Any] = {}

    role = optional_attr(credentials, "role")
    if role is not None:
        additional_properties["server_settings"] = {"role": role}

    for credentials_key, additional_properties_key in [
        ("sslmode", "sslmode"),
        ("sslcert", "sslcert"),
        ("sslkey", "sslkey"),
        ("sslrootcert", "sslrootcert"),
    ]:
        additional_properties.update(
            _get_additional_property(
                credentials, credentials_key=credentials_key, additional_properties_key=additional_properties_key
            )
        )

    return additional_properties


def build_snowflake_config(
    credentials: Any, datasource_name: str, datasource_type: DatasourceType
) -> SnowflakeConfigFile:
    return SnowflakeConfigFile(
        name=datasource_name,
        type=datasource_type.full_type,
        connection=SnowflakeConnectionProperties(
            account=require_attr(credentials, "account"),
            warehouse=optional_attr(credentials, "warehouse"),
            database=optional_attr(credentials, "database"),
            user=optional_attr(credentials, "user"),
            role=optional_attr(credentials, "role"),
            auth=map_snowflake_auth(credentials),
            additional_properties=map_snowflake_additional_properties(credentials),
        ),
    )


def map_snowflake_additional_properties(credentials: Any) -> dict[str, Any]:
    additional_properties: dict[str, Any] = {}

    for credentials_key, additional_properties_key in [
        ("host", "host"),
        ("port", "port"),
        ("protocol", "protocol"),
        ("proxy_host", "proxy_host"),
        ("proxy_port", "proxy_port"),
        ("insecure_mode", "insecure_mode"),
    ]:
        additional_properties.update(
            _get_additional_property(
                credentials, credentials_key=credentials_key, additional_properties_key=additional_properties_key
            )
        )

    return additional_properties


def map_snowflake_auth(
    credentials: Any,
) -> SnowflakePasswordAuth | SnowflakeKeyPairAuth | SnowflakeSSOAuth | SnowflakeOAuthAuth:
    token = optional_attr(credentials, "token")
    authenticator = optional_attr(credentials, "authenticator")
    private_key = optional_attr(credentials, "private_key")
    private_key_file = optional_attr(credentials, "private_key_file", "private_key_path")
    private_key_file_pwd = optional_attr(credentials, "private_key_file_pwd", "private_key_passphrase")
    password = optional_attr(credentials, "password")

    if token:
        return SnowflakeOAuthAuth(token=token)
    if private_key or private_key_file:
        return SnowflakeKeyPairAuth(
            private_key=private_key,
            private_key_file=private_key_file,
            private_key_file_pwd=private_key_file_pwd,
        )
    if authenticator and authenticator != "snowflake":
        return SnowflakeSSOAuth(authenticator=authenticator)
    if password:
        return SnowflakePasswordAuth(password=password)
    if authenticator in (None, "snowflake"):
        raise ValueError(
            "Snowflake credentials are using the default password authenticator, but no password was provided."
        )

    raise ValueError(
        "Unsupported Snowflake authentication configuration. Supported auth modes: password, key pair, authenticator/SSO, token"
    )


def build_sqlite_config(credentials: Any, datasource_name: str, datasource_type: DatasourceType) -> SQLiteConfigFile:
    return SQLiteConfigFile(
        name=datasource_name,
        type=datasource_type.full_type,
        connection=SQLiteConnectionConfig(database_path=resolve_sqlite_database_path(credentials)),
    )


def resolve_sqlite_database_path(credentials: Any) -> str:
    direct_path = optional_attr(credentials, "database_path")
    if direct_path:
        return str(direct_path)

    schemas_and_paths = optional_attr(credentials, "schemas_and_paths")
    if isinstance(schemas_and_paths, Mapping) and schemas_and_paths:
        schema_name = optional_attr(credentials, "schema")
        if isinstance(schema_name, str) and schema_name in schemas_and_paths:
            return str(schemas_and_paths[schema_name])
        if "main" in schemas_and_paths:
            return str(schemas_and_paths["main"])
        first_available_path = str(next(iter(schemas_and_paths.values())))
        available_schemas = ", ".join(sorted(str(key) for key in schemas_and_paths))
        raise ValueError(
            f"Could not resolve SQLite database path for schema '{schema_name}'. "
            f"No direct database_path was provided, no 'main' schema was present, and the available schemas were: {available_schemas}. "
            f"The first available path would have been: {first_available_path}"
        )

    raise ValueError(
        "Could not resolve SQLite database path from dbt credentials. Expected database_path or a non-empty schemas_and_paths mapping."
    )


def build_context_from_dbt_project(
    project_path: Path,
    datasource_name: str,
    plugin_loader: DatabaoContextPluginLoader | None = None,
) -> BuiltDatasourceContext[Any]:
    profile = load_dbt_profile(project_path)
    datasource_config = build_datasource_config_from_profile(profile, datasource_name)
    datasource_type = DatasourceType(full_type=datasource_config.type)
    loader = plugin_loader or DatabaoContextPluginLoader()
    plugin = loader.get_plugin_for_datasource_type(datasource_type)

    if plugin is None:
        raise ValueError(f"No plugin found for datasource type '{datasource_type.full_type}'")
    if not isinstance(plugin, BaseDatabasePlugin):
        raise ValueError(f"Resolved plugin for '{datasource_type.full_type}' is not a datasource plugin")

    built_context = execute_datasource_plugin(
        plugin=plugin,
        datasource_type=datasource_type,
        config=datasource_config.model_dump(exclude_none=True, by_alias=True),
        datasource_name=datasource_name,
    )
    return BuiltDatasourceContext(
        datasource_id=f"{datasource_name}.yaml",
        datasource_type=datasource_type.full_type,
        context=built_context,
    )


def extract_dbt_adapter_type(credentials: Any) -> str | None:
    adapter_type = getattr(credentials, "type", None)
    if isinstance(adapter_type, str):
        return adapter_type
    if callable(adapter_type):
        value = adapter_type()
        return value if isinstance(value, str) else None
    return None


def require_attr(obj: Any, *names: str) -> Any:
    value = optional_attr(obj, *names)
    if value is None:
        joined_names = ", ".join(names)
        raise ValueError(f"Resolved dbt credentials are missing required field(s): {joined_names}")
    return value


def optional_attr(obj: Any, *names: str) -> Any:
    for name in names:
        if hasattr(obj, name):
            value = getattr(obj, name)
            if value is not None:
                return value
    return None


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    project_path = validate_project_path(args.project_path)
    datasource_name = args.datasource_name or project_path.name

    built_context = build_context_from_dbt_project(project_path, datasource_name)
    sys.stdout.write(to_yaml_string(built_context))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
