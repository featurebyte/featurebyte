"""
Test typing module
"""
from featurebyte.common.typing import get_or_default


def test_get_or_default():
    """
    Test get_or_default
    """
    value = get_or_default(None, "a")
    assert value == "a"

    value = get_or_default("b", "a")
    assert value == "b"
