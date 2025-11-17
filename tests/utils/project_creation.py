import shutil
from pathlib import Path
from typing import Collection

from nemory.project.layout import SOURCE_FOLDER_NAME


def copy_resources_as_datasources(project_dir: Path, resources: Collection[tuple[str, str]]) -> None:
    for folder_name, resource_name in resources:
        copy_resource_as_datasource(project_dir, folder_name, resource_name)


def copy_resource_as_datasource(project_dir: Path, folder_name: str, resource_name: str) -> None:
    datasource_folder = project_dir.joinpath(SOURCE_FOLDER_NAME).joinpath(folder_name)
    datasource_folder.mkdir(exist_ok=True)

    shutil.copy2(Path(__file__).parent.parent.joinpath("resources").joinpath(resource_name), datasource_folder)
