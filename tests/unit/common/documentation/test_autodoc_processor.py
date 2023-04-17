"""
Test autodoc processor
"""
import pytest

from featurebyte.common.documentation.autodoc_processor import (
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
