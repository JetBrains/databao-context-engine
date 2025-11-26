from datetime import datetime
from typing import Any

import yaml

from nemory.pluginlib.build_plugin import DefaultBuildDatasourcePlugin, BuildExecutionResult
from nemory.pluginlib.plugin_execution import execute_datasource_plugin


class DummyDefaultDatasourcePlugin(DefaultBuildDatasourcePlugin):
    def execute(self, full_type: str, datasource_name: str, file_config: dict[str, Any]) -> BuildExecutionResult:
        return BuildExecutionResult(
            id="dummy",
            name=datasource_name,
            type=full_type,
            result={"ok": True},
            version="1.0",
            executed_at=datetime.now(),
            description=None,
        )


def test_default_build_datasource_plugin():
    config_yaml = yaml.safe_load(
        """
        my_key: my_value
        """
    )

    result = execute_datasource_plugin(DummyDefaultDatasourcePlugin(), "my_type", config_yaml, "datasource_name")

    assert result.result == {"ok": True}
