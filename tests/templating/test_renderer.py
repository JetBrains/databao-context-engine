import yaml

from nemory.templating.renderer import render_template


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
