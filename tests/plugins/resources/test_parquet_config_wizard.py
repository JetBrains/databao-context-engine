from pathlib import Path

from databao_context_engine import DatabaoContextDomainManager, DatabaoContextPluginLoader, DatasourceType
from databao_context_engine.plugins.resources.parquet_plugin import ParquetPlugin
from tests.utils.config_wizard import MockUserInputCallback


def test_add_parquet_datasource_config(project_path: Path):
    plugin_loader = DatabaoContextPluginLoader(
        plugins_by_type={
            DatasourceType(full_type="parquet"): ParquetPlugin(),
        }
    )
    project_manager = DatabaoContextDomainManager(domain_dir=project_path, plugin_loader=plugin_loader)

    inputs = [
        "my_url_to_file.parquet",  # url
        False,  # is_local_file
    ]
    user_input_callback = MockUserInputCallback(inputs=inputs)

    configured_datasource = project_manager.create_datasource_config_interactively(
        datasource_type=DatasourceType(full_type="parquet"),
        datasource_name="res/my_parq",
        user_input_callback=user_input_callback,
        validate_config_content=False,
    )

    assert configured_datasource.config == {
        "type": "parquet",
        "name": "my_parq",
        "url": "my_url_to_file.parquet",
    }
