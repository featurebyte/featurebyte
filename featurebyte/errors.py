"""
Module for featurebyte errors
"""


class MissingPointInTimeColumnError(Exception):
    """Raised when point in time column is not provided in historical requests"""


class TooRecentPointInTimeError(Exception):
    """Raised when the latest point in time value is too recent in historical requests"""
