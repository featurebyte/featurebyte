"""
This module contains all the enums used for session specific tasks.
"""

from featurebyte.enum import StrEnum


class SnowflakeDataType(StrEnum):
    """
    Snowflake data type
    """

    FIXED = "FIXED"
    REAL = "REAL"
    TEXT = "TEXT"
    BINARY = "BINARY"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIME = "TIME"
    ARRAY = "ARRAY"
    OBJECT = "OBJECT"
    TIMESTAMP_LTZ = "TIMESTAMP_LTZ"
    TIMESTAMP_NTZ = "TIMESTAMP_NTZ"
    TIMESTAMP_TZ = "TIMESTAMP_TZ"
