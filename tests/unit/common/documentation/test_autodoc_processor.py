"""
Test autodoc processor
"""
import pytest

from featurebyte.common.documentation.autodoc_processor import (
    ParameterDetails,
    _get_parameters_content,
    _render_list_item_with_multiple_paragraphs,
)


@pytest.mark.parametrize(
    "input_title,input_paragraphs,expected_output",
    [
        ("", [], ""),
        ("", ["a"], ""),
        ("title", [], "- **title**<br><br>"),
        ("title", ["a", "b"], "- **title**<br>\ta<br>\tb<br><br>"),
    ],
)
def test_render_list_item_with_multiple_paragraphs(input_title, input_paragraphs, expected_output):
    """
    Test _render_list_item_with_multiple_paragraphs
    """
    rendered_str = _render_list_item_with_multiple_paragraphs(input_title, input_paragraphs)
    assert rendered_str == expected_output


@pytest.mark.parametrize(
    "parameters,expected_output",
    [
        ([], ""),
        ([ParameterDetails(name=None, type=None, default=None, description=None)], ""),
        (
            [
                ParameterDetails(
                    name="name", type="int", default="Float", description="test description"
                ),
                ParameterDetails(
                    name="name2", type="int2", default="Float2", description="test description2"
                ),
            ],
            "- **name: *int***<br>\t**default**: *Float*\n<br>\ttest description<br><br>\n"
            "- **name2: *int2***<br>\t**default**: *Float2*\n<br>\ttest description2<br><br>\n",
        ),
        (
            [
                ParameterDetails(
                    name="name", type=None, default="Float", description="test description"
                )
            ],
            "- **name**<br>\t**default**: *Float*\n<br>\ttest description<br><br>\n",
        ),
        (
            [ParameterDetails(name="name", type="int", default="Float", description=None)],
            "- **name: *int***<br>\t**default**: *Float*\n<br><br>\n",
        ),
    ],
)
def test_get_parameters_content(parameters, expected_output):
    """
    Test _get_parameters_content
    """
    content = _get_parameters_content(parameters)
    assert content == expected_output
