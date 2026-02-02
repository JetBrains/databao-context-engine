import os

import pytest
import yaml

from databao_context_engine.project.layout import ProjectLayout
from databao_context_engine.templating.renderer import UnknownEnvVarTemplateError, render_template


@pytest.fixture
def unvalidated_project_layout(tmp_path):
    tmp_project_dir = tmp_path.joinpath("unvalidated_project_dir")

    return ProjectLayout(project_dir=tmp_project_dir, config_file=tmp_project_dir.joinpath("unused_config.ini"))


def test_render_template__plain_yaml(unvalidated_project_layout: ProjectLayout):
    yaml_content = """
    attribute:
        subattribute: "test"
        second_attribute: 123
    """

    expected = {"attribute": {"subattribute": "test", "second_attribute": 123}}

    assert yaml.safe_load(yaml_content) == expected

    result = render_template(unvalidated_project_layout, yaml_content)

    assert yaml.safe_load(result) == expected


def test_render_template__with_simple_calculation(unvalidated_project_layout: ProjectLayout):
    yaml_content = """
    attribute:
        subattribute: "test"
        second_attribute: {{ 123 + 1 }}
    """

    expected = {"attribute": {"subattribute": "test", "second_attribute": 124}}

    result = render_template(unvalidated_project_layout, yaml_content)

    assert yaml.safe_load(result) == expected


def test_render_template__with_env_variable(unvalidated_project_layout: ProjectLayout):
    os.environ["DCE_DATASOURCE_PASSWORD"] = "mypassword"

    yaml_content = """
    attribute:
        subattribute: "test"
        second_attribute: {{ 123 + 1 }}
    secret: {{ env_var('DCE_DATASOURCE_PASSWORD') }}
    """

    expected = {"attribute": {"subattribute": "test", "second_attribute": 124}, "secret": "mypassword"}

    result = render_template(unvalidated_project_layout, yaml_content)

    assert yaml.safe_load(result) == expected


def test_render_template__with_env_variable_default(unvalidated_project_layout: ProjectLayout):
    os.environ["DCE_DATASOURCE_PASSWORD"] = "mypassword"

    yaml_content = """
    attribute:
        subattribute: "test"
        second_attribute: {{ 123 + 1 }}
    user: {{ env_var('DCE_DATASOURCE_USER', 'default_user') }}
    secret: {{ env_var('DCE_DATASOURCE_PASSWORD', 'default_password') }}
    """

    expected = {
        "attribute": {"subattribute": "test", "second_attribute": 124},
        "user": "default_user",
        "secret": "mypassword",
    }

    result = render_template(unvalidated_project_layout, yaml_content)

    assert yaml.safe_load(result) == expected


def test_render_template__fails_if_env_variable_missing_and_no_default(unvalidated_project_layout: ProjectLayout):
    os.environ["DCE_DATASOURCE_PASSWORD"] = "mypassword"

    yaml_content = """
    attribute:
        subattribute: "test"
        second_attribute: {{ 123 + 1 }}
    user: {{ env_var('DCE_DATASOURCE_USER') }}
    secret: {{ env_var('DCE_DATASOURCE_PASSWORD', 'default_password') }}
    """

    with pytest.raises(UnknownEnvVarTemplateError):
        render_template(unvalidated_project_layout, yaml_content)


def test_render_template__with_project_dir_variable(unvalidated_project_layout: ProjectLayout):
    yaml_content = """
    attribute:
        subattribute: "test"
        second_attribute: {{ 123 + 1 }}
    path_relative_to_project: {{ PROJECT_DIR }}/my_file.txt
    path_relative_to_src: {{ SRC_DIR }}/my_file.txt
    """

    expected = {
        "attribute": {"subattribute": "test", "second_attribute": 124},
        "path_relative_to_project": str(unvalidated_project_layout.project_dir.joinpath("my_file.txt").resolve()),
        "path_relative_to_src": str(unvalidated_project_layout.src_dir.joinpath("my_file.txt").resolve()),
    }

    result = render_template(unvalidated_project_layout, yaml_content)

    assert yaml.safe_load(result) == expected
