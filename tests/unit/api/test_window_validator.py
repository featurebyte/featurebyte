"""
Test window validator
"""

import pytest

from featurebyte.api.window_validator import validate_window


def test_validate_window__in_proper_range():
    """
    Test _is_window_in_proper_range
    """

    # less than should error
    with pytest.raises(ValueError) as exc:
        validate_window("14m", "15m")
    assert "greater than the feature_job_frequency" in str(exc)

    # exactly same shouldn't error
    validate_window("15m", "15m")
    # greater than shouldn't error
    validate_window("30m", "15m")


@pytest.mark.parametrize(
    "window, feature_job_period, expected_error",
    [
        ("600d", "15m", "window 600d needs to be less than 525 days"),
        ("1200d", "30m", "window 1200d needs to be less than 1050 days"),
        ("2500d", "1h", "window 2500d needs to be less than 2100 days"),
        ("180d", "1h", None),
        ("1000d", "1d", None),
        ("60000d", "1d", "window 60000d needs to be less than 50400 days"),
    ],
)
def test_validate_window__upper_limit(window, feature_job_period, expected_error):
    """
    Test upper limit of window
    """
    # greater than upper bound should error
    if expected_error is not None:
        with pytest.raises(ValueError) as exc:
            validate_window(window, feature_job_period)
        full_error = f"{expected_error}. Please specify a different time window or increase the feature job period."
        assert full_error in str(exc)
    else:
        validate_window(window, feature_job_period)


def test_validates_window__not_multiple_of_feature_job_frequency():
    """
    Test _is_window_multiple_of_feature_job_frequency
    """
    validate_window("15m", "5m")

    # not a multiple should throw error
    with pytest.raises(ValueError) as exc:
        validate_window("14m", "10m")
    assert "not a multiple of the feature job frequency" in str(exc)


def test_validate_window():
    """
    Test validate_window

    Basic tests here to just illustrate that we do throw some errors. Most of the testing will be handled in other
    unit tests.
    """
    # invalid input throws error
    with pytest.raises(ValueError) as exc:
        validate_window("random_str", "random_str_2")
    assert "unit abbreviation" in str(exc)

    # valid input is ok
    validate_window("15m", "5m")
