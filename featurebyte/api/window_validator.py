"""
Validate window parameter input.
"""
from typing import Optional

from featurebyte.common.model_util import parse_duration_string, validate_offset_string


def validate_offset(offset: Optional[str]) -> None:
    """
    Validates whether the offset param is a valid one.

    Parameters
    ----------
    offset: str
        the offset parameter string
    """
    # Validate offset is valid if provided
    if offset is not None:
        validate_offset_string(offset)


def validate_window(window: str, feature_job_frequency: str) -> None:
    """
    Validates whether the window param is a valid one when used in an aggregate over function.

    In particular, we check if
    - window strings are valid
    - window sizes are multiples of the feature job frequency
    - window strings are in proper range (between feature job frequency and some upper limit e.g 10000 years

    Parameters
    ----------
    window: str
        the window parameter string
    feature_job_frequency: str
        feature job frequency

    Raises
    ------
    ValueError
        raised when validation fails
    """
    # parse and validate input
    window_secs = parse_duration_string(window)
    feature_job_frequency_secs = parse_duration_string(feature_job_frequency)

    if window_secs < feature_job_frequency_secs:
        raise ValueError(
            f"window provided {window} needs to be greater than the feature_job_frequency {feature_job_frequency}"
        )

    # arbitrary upper bound of 365 days
    upper_bound_timerange_secs = 365 * 24 * 60 * 60
    if window_secs > upper_bound_timerange_secs:
        raise ValueError(
            f"window {window} needs to be less than 365 days. Please specify a different time window."
        )

    time_mod = window_secs % feature_job_frequency_secs
    if time_mod != 0:
        raise ValueError(
            f"window provided {window} is not a multiple of the feature job frequency {feature_job_frequency}"
        )
