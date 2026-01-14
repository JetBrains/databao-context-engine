import os

import pytest
import yaml

from nemory.templating.renderer import UnknownEnvVarTemplateError, render_template


def test_render_template__plain_yaml():
    yaml_content = """
    attribute:
        subattribute: "test"
        second_attribute: 123
    """

    expected = {"attribute": {"subattribute": "test", "second_attribute": 123}}

    assert yaml.safe_load(yaml_content) == expected

    result = render_template(yaml_content)

    assert yaml.safe_load(result) == expected


def test_render_template__with_simple_calculation():
    yaml_content = """
    attribute:
        subattribute: "test"
        second_attribute: {{ 123 + 1 }}
    """

    expected = {"attribute": {"subattribute": "test", "second_attribute": 124}}

    result = render_template(yaml_content)

    assert yaml.safe_load(result) == expected


def test_render_template__with_env_variable():
    os.environ["DCE_DATASOURCE_PASSWORD"] = "mypassword"

    yaml_content = """
    attribute:
        subattribute: "test"
        second_attribute: {{ 123 + 1 }}
    secret: {{ env_var('DCE_DATASOURCE_PASSWORD') }}
    """

    expected = {"attribute": {"subattribute": "test", "second_attribute": 124}, "secret": "mypassword"}

    result = render_template(yaml_content)

    assert yaml.safe_load(result) == expected


def test_render_template__with_env_variable_default():
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

    result = render_template(yaml_content)

    assert yaml.safe_load(result) == expected


def test_render_template__fails_if_env_variable_missing_and_no_default():
    os.environ["DCE_DATASOURCE_PASSWORD"] = "mypassword"

    yaml_content = """
    attribute:
        subattribute: "test"
        second_attribute: {{ 123 + 1 }}
    user: {{ env_var('DCE_DATASOURCE_USER') }}
    secret: {{ env_var('DCE_DATASOURCE_PASSWORD', 'default_password') }}
    """

    with pytest.raises(UnknownEnvVarTemplateError):
        render_template(yaml_content)
