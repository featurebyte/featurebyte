"""
Tests for functions in dict_util.py module
"""
import pytest

from featurebyte.common.dict_util import get_field_path_value


@pytest.mark.parametrize(
    "doc, field_path, expected",
    [
        ({"a": [1]}, [], {"a": [1]}),
        ({"a": [1]}, ["a"], [1]),
        ({"a": [1]}, ["a", 0], 1),
        ({"a": {"b": {"c": {"d": [123]}}}}, ["a", "b", "c", "d", 0], 123),
        ({"a": [{"b": {"c": [{"d": [123]}]}}]}, ["a", 0, "b", "c", 0, "d", 0], 123),
    ],
)
def test_field_path_value(doc, field_path, expected):
    """Test field_path_value logic"""
    assert get_field_path_value(doc_dict=doc, field_path=field_path) == expected
