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

    # greater than upper bound should error
    with pytest.raises(ValueError) as exc:
        validate_window("366d", "15m")
    assert "window 366d needs to be less than 365 days" in str(exc)


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
