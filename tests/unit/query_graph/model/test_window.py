"""
Tests for featurebyte/query_graph/model/window.py
"""

import pytest

from featurebyte.query_graph.model.window import CalendarWindow


@pytest.mark.parametrize(
    "window, expected",
    [
        (CalendarWindow(unit="MINUTE", size=2), 120),
        (CalendarWindow(unit="HOUR", size=2), 7200),
        (CalendarWindow(unit="DAY", size=2), 172800),
        (CalendarWindow(unit="WEEK", size=2), 1209600),
    ],
)
def test_to_seconds(window, expected):
    """
    Test to_seconds method of CalendarWindow
    """
    assert window.is_fixed_size()
    assert window.to_seconds() == expected


@pytest.mark.parametrize(
    "window, expected",
    [
        (CalendarWindow(unit="MONTH", size=2), 2),
        (CalendarWindow(unit="QUARTER", size=2), 6),
        (CalendarWindow(unit="YEAR", size=2), 24),
    ],
)
def test_to_months(window, expected):
    """
    Test to_months method of CalendarWindow
    """
    assert not window.is_fixed_size()
    assert window.to_months() == expected
