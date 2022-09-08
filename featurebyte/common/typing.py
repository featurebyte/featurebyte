"""
Common utilities related to typing
"""
from __future__ import annotations

from typing import Literal

TimedeltaSupportedUnitType = Literal[
    "day",
    "hour",
    "minute",
    "second",
    "millisecond",
    "microsecond",
]
