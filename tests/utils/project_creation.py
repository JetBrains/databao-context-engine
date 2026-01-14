from pathlib import Path
from typing import Any

from nemory.pluginlib.build_plugin import DatasourceType
from nemory.project.layout import create_datasource_config_file
from nemory.serialisation.yaml import to_yaml_string


def with_config_file(project_dir: Path, full_type: str, datasource_name: str, config_content: dict[str, Any]) -> Path:
    return create_datasource_config_file(
        project_dir=project_dir,
        datasource_type=DatasourceType(full_type=full_type),
        datasource_name=datasource_name,
        config_content=to_yaml_string(config_content),
    )
