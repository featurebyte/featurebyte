"""Tests for feature job setting validation"""

import pytest

from featurebyte.common.model_util import (
    convert_seconds_to_time_format,
    convert_version_string_to_dict,
    validate_job_setting_parameters,
    validate_timezone_offset_string,
)


def test_offset_larger_than_frequency():
    """Test that offset should be larger than frequency"""
    with pytest.raises(ValueError) as exc_info:
        validate_job_setting_parameters(period="1h", offset="2h", blind_spot="5m")
    expected = "Offset (2h) should be smaller than period (1h)"
    assert expected in str(exc_info)


def test_blind_spot_should_be_positive():
    """Test that blind spot should be positive"""
    with pytest.raises(ValueError) as exc_info:
        validate_job_setting_parameters(period="1h", offset="2h", blind_spot="-10m")
    expected = "Duration specified is too small: -10m"
    assert expected in str(exc_info)


def test_frequency_should_at_least_one_minute():
    """Test that frequency should be at least one minute"""
    with pytest.raises(ValueError) as exc_info:
        validate_job_setting_parameters(period="5s", offset="1s", blind_spot="2s")
    expected = "Duration specified is too small: 5s"
    assert expected in str(exc_info)


@pytest.mark.parametrize(
    "input_value, expected",
    [
        ("V220920", {"name": "V220920", "suffix": None}),
        ("V220920_1", {"name": "V220920", "suffix": 1}),
    ],
)
def test_convert_version_string_to_dict(input_value, expected):
    """Test convert version string to dictionary"""
    assert convert_version_string_to_dict(input_value) == expected


@pytest.mark.parametrize(
    "timezone_offset",
    [
        "+08:00",
        "-05:30",
    ],
)
def test_validate_timezone_offset_string__valid(timezone_offset):
    """
    Test validate_timezone_offset_string on valid offset strings
    """
    validate_timezone_offset_string(timezone_offset)


@pytest.mark.parametrize(
    "timezone_offset",
    [
        "0800",
        "08:00",
        "+00:61",
        "+25:00",
        "+ab:cd",
        "+08:00:00",
    ],
)
def test_validate_timezone_offset_string__invalid(timezone_offset):
    """
    Test validate_timezone_offset_string on valid offset strings
    """
    with pytest.raises(ValueError) as exc:
        validate_timezone_offset_string(timezone_offset)
    assert "Invalid timezone_offset" in str(exc.value)


@pytest.mark.parametrize(
    "seconds, components, expected",
    [
        # Test cases without specifying components (default behavior)
        (45, None, "45s"),
        (100, None, "1m40s"),
        (3600, None, "1h"),
        (3660, None, "1h1m"),
        (3665, None, "1h1m5s"),
        (86400, None, "1d"),
        (86465, None, "1d1m5s"),
        (90061, None, "1d1h1m1s"),
        (90065, None, "1d1h1m5s"),
        # Test cases with specified components
        (90065, 1, "1d"),
        (90065, 2, "1d1h"),
        (90065, 3, "1d1h1m"),
        (90065, 4, "1d1h1m5s"),
        (3665, 2, "1h1m"),
        (3665, 1, "1h"),
    ],
)
def test_convert_seconds_to_time_format(seconds, components, expected):
    """
    Test convert_seconds_to_time_format with and without specified components
    """
    if components is not None:
        assert convert_seconds_to_time_format(seconds, components) == expected
    else:
        assert convert_seconds_to_time_format(seconds) == expected
