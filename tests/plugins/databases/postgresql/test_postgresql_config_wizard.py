from pathlib import Path

from databao_context_engine import DatabaoContextDomainManager, DatabaoContextPluginLoader, DatasourceType
from databao_context_engine.plugins.databases.postgresql.postgresql_db_plugin import PostgresqlDbPlugin
from tests.utils.config_wizard import MockUserInputCallback


def test_add_parquet_datasource_config_with_all_defaults(project_path: Path):
    plugin_loader = DatabaoContextPluginLoader(
        plugins_by_type={
            DatasourceType(full_type="postgres"): PostgresqlDbPlugin(),
        }
    )
    project_manager = DatabaoContextDomainManager(domain_dir=project_path, plugin_loader=plugin_loader)

    inputs = [
        "",  # host
        "",  # port
        "",  # database
        "",  # user
        "",  # password
    ]
    user_input_callback = MockUserInputCallback(inputs=inputs)

    configured_datasource = project_manager.create_datasource_config_interactively(
        datasource_type=DatasourceType(full_type="postgres"),
        datasource_name="res/my_pg",
        user_input_callback=user_input_callback,
        validate_config_content=True,
    )

    assert configured_datasource.config == {"connection": {"host": "localhost"}, "name": "my_pg", "type": "postgres"}
