"""
Test validator module
"""
import pytest

from featurebyte.common.validator import duration_string_validator


@pytest.mark.parametrize(
    "duration_value,is_valid",
    [
        ("1s", True),
        ("1m", True),
        ("1h", True),
        ("1d", True),
        ("1w", True),
        ("1y", True),
        ("random", False),
    ],
)
def test_duration_string_validator(duration_value, is_valid):
    """
    Test duration_string_validator
    """
    if is_valid:
        assert duration_string_validator(None, duration_value) == duration_value
    else:
        with pytest.raises(ValueError):
            duration_string_validator(None, duration_value)
