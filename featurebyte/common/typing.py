"""
Common utilities related to typing
"""
from __future__ import annotations

from typing import Any, Literal

import pandas as pd
from pandas.api.types import is_scalar

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
    return pd.isna(value)
