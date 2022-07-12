"""
Helper functions
"""
from __future__ import annotations

import datetime


def get_utc_now() -> datetime.datetime:
    """
    Return truncated current datetime object
    """
    # exclude microseconds from timestamp as it's not supported in persistent
    utc_now = datetime.datetime.utcnow()
    utc_now = utc_now.replace(microsecond=int(utc_now.microsecond / 1000) * 1000)
    return utc_now
