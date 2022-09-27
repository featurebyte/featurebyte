"""
Common utilities related to typing
"""
from __future__ import annotations

from typing import Any, Literal, cast

import pandas as pd
from pandas.api.types import is_scalar

DatetimeSupportedPropertyType = Literal[
    "year",
    "quarter",
    "month",
    "week",
    "day",
    "dayofweek",
    "hour",
    "minute",
    "second",
]
TimedeltaSupportedUnitType = Literal[
    "day",
    "hour",
    "minute",
    "second",
    "millisecond",
    "microsecond",
]


def is_scalar_nan(value: Any) -> bool:
    """
    Returns whether the provided value is a scalar nan value (float('nan'), np.nan, None)

    Parameters
    ----------
    value : Any
        Value to check

    Returns
    -------
    bool
    """
    if not is_scalar(value):
        return False
    return cast(bool, pd.isna(value))
