"""Tests for feature job setting validation
"""
import pytest

from featurebyte.common.model_util import (
    convert_version_string_to_dict,
    validate_job_setting_parameters,
    validate_timezone_offset_string,
)


def test_time_modulo_frequency_larger_than_frequency():
    """Test that time modulo frequency should be larger than frequency"""
    with pytest.raises(ValueError) as exc_info:
        validate_job_setting_parameters(frequency="1h", time_modulo_frequency="2h", blind_spot="5m")
    expected = "Time modulo frequency (2h) should be smaller than frequency (1h)"
    assert expected in str(exc_info)


def test_blind_spot_should_be_positive():
    """Test that blind spot should be positive"""
    with pytest.raises(ValueError) as exc_info:
        validate_job_setting_parameters(
            frequency="1h", time_modulo_frequency="2h", blind_spot="-10m"
        )
    expected = "Duration specified is too small: -10m"
    assert expected in str(exc_info)


def test_frequency_should_at_least_one_minute():
    """Test that frequency should be at least one minute"""
    with pytest.raises(ValueError) as exc_info:
        validate_job_setting_parameters(frequency="5s", time_modulo_frequency="1s", blind_spot="2s")
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
