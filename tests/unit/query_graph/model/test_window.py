"""
Tests for featurebyte/query_graph/model/window.py
"""

import pytest

from featurebyte.query_graph.model.window import FeatureWindow


@pytest.mark.parametrize(
    "window, expected",
    [
        (FeatureWindow(unit="MINUTE", size=2), 120),
        (FeatureWindow(unit="HOUR", size=2), 7200),
        (FeatureWindow(unit="DAY", size=2), 172800),
        (FeatureWindow(unit="WEEK", size=2), 1209600),
    ],
)
def test_to_seconds(window, expected):
    """
    Test to_seconds method of FeatureWindow
    """
    assert window.is_fixed_size()
    assert window.to_seconds() == expected


@pytest.mark.parametrize(
    "window, expected",
    [
        (FeatureWindow(unit="MONTH", size=2), 2),
        (FeatureWindow(unit="QUARTER", size=2), 6),
        (FeatureWindow(unit="YEAR", size=2), 24),
    ],
)
def test_to_months(window, expected):
    """
    Test to_months method of FeatureWindow
    """
    assert not window.is_fixed_size()
    assert window.to_months() == expected
