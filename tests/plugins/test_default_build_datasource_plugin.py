import yaml

from databao_context_engine.pluginlib.build_plugin import DatasourceType
from databao_context_engine.pluginlib.plugin_utils import execute_datasource_plugin
from tests.utils.dummy_build_plugin import DummyDefaultDatasourcePlugin


def test_default_build_datasource_plugin():
    config_yaml = yaml.safe_load(
        """
        my_key: my_value
        """
    )

    result = execute_datasource_plugin(
        DummyDefaultDatasourcePlugin(), DatasourceType(full_type="dummy/dummy_default"), config_yaml, "datasource_name"
    )

    assert result == {"ok": True}
