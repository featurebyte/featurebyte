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


def test_comparison__fixed_size():
    """
    Test comparison methods of CalendarWindow
    """
    assert CalendarWindow(unit="DAY", size=2) == CalendarWindow(unit="DAY", size=2)
    assert CalendarWindow(unit="DAY", size=2) == CalendarWindow(unit="HOUR", size=48)
    assert CalendarWindow(unit="DAY", size=2) > CalendarWindow(unit="HOUR", size=36)
    assert CalendarWindow(unit="DAY", size=2) < CalendarWindow(unit="HOUR", size=72)
    assert max([
        CalendarWindow(unit="DAY", size=1),
        CalendarWindow(unit="HOUR", size=48),
    ]) == CalendarWindow(unit="HOUR", size=48)


def test_comparison__non_fixed_size():
    """
    Test comparison methods of CalendarWindow
    """
    assert CalendarWindow(unit="MONTH", size=3) == CalendarWindow(unit="MONTH", size=3)
    assert CalendarWindow(unit="MONTH", size=3) == CalendarWindow(unit="QUARTER", size=1)
    assert CalendarWindow(unit="MONTH", size=6) > CalendarWindow(unit="QUARTER", size=1)
    assert CalendarWindow(unit="MONTH", size=3) < CalendarWindow(unit="QUARTER", size=2)
    assert max([
        CalendarWindow(unit="MONTH", size=2),
        CalendarWindow(unit="QUARTER", size=1),
    ]) == CalendarWindow(unit="QUARTER", size=1)


def test_comparison__invalid():
    """
    Test comparison methods of CalendarWindow
    """
    with pytest.raises(ValueError) as exc_info:
        _ = CalendarWindow(unit="DAY", size=2) < CalendarWindow(unit="MONTH", size=2)
    assert str(exc_info.value) == "Cannot compare a fixed size window with a non-fixed size window"
