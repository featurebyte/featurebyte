"""
Validate window parameter input.
"""

import pandas as pd

from featurebyte.common.model_util import validate_offset_string


def _is_window_multiple_of_feature_job_frequency(window: str, feature_job_frequency: str) -> None:
    """
    Validates whether the window param is a multiple of a feature job frequency.

    Parameters
    ----------
    window: str
        the window parameter string
    feature_job_frequency: str
        feature job frequency

    Raises
    ------
    ValueError
        raised if the window string is not a multiple of a feature job frequency.
    """
    window_timedelta = pd.Timedelta(window)
    feature_job_frequency_timeldeta = pd.Timedelta(feature_job_frequency)

    time_mod = window_timedelta % feature_job_frequency_timeldeta
    if time_mod.delta != 0:
        raise ValueError(
            f"window provided {window} is not a multiple of the feature job frequency {feature_job_frequency}"
        )


def _is_window_in_proper_range(window: str, feature_job_frequency: str) -> None:
    """
    Validates whether the window param is in a proper range.

    Parameters
    ----------
    window: str
        the window parameter string
    feature_job_frequency: str
        feature job frequency

    Raises
    ------
    ValueError
        raised if the window string is not a multiple of a feature job frequency.
    """
    window_timerange = pd.Timedelta(window)
    frequency_timerange = pd.Timedelta(feature_job_frequency)
    if window_timerange < frequency_timerange:
        raise ValueError(
            f"window provided {window} needs to be greater than the feature_job_frequency {feature_job_frequency}"
        )

    upper_bound_timerange = pd.Timedelta("100y")
    if window_timerange > upper_bound_timerange:
        raise ValueError(
            "window needs to be less than 100y. Please specify a different time window."
        )


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
    """
    validate_offset_string(window)
    validate_offset_string(feature_job_frequency)
    _is_window_multiple_of_feature_job_frequency(window, feature_job_frequency)
    _is_window_in_proper_range(window, feature_job_frequency)
