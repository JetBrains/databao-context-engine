import yaml

from nemory.pluginlib.plugin_utils import execute_datasource_plugin
from tests.utils.dummy_build_plugin import DummyDefaultDatasourcePlugin


def test_default_build_datasource_plugin():
    config_yaml = yaml.safe_load(
        """
        my_key: my_value
        """
    )

    result = execute_datasource_plugin(DummyDefaultDatasourcePlugin(), "my_type", config_yaml, "datasource_name")

    assert result.result == {"ok": True}
