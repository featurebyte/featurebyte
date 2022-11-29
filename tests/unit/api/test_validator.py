"""
Test validator class
"""
import pytest

from featurebyte.api.validator import validate_view
from featurebyte.exception import JoinViewMismatchError


def test_validate_view(snowflake_scd_view, snowflake_dimension_view, snowflake_event_view):
    """
    Test validate view
    """
    # No error expected
    validate_view(snowflake_dimension_view)

    # Error expected
    with pytest.raises(JoinViewMismatchError):
        validate_view(snowflake_event_view)
    with pytest.raises(JoinViewMismatchError):
        validate_view(snowflake_scd_view)
