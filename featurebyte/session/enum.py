"""
This module contains all the enums used for session specific tasks.
"""
from enum import Enum


class SnowflakeDataType(str, Enum):
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
    TIMESTAMP_LTZ = "TIMESTAMP_LTZ"
    TIMESTAMP_NTZ = "TIMESTAMP_NTZ"
    TIMESTAMP_TZ = "TIMESTAMP_TZ"


class SourceType(str, Enum):
    """
    Database or data warehouse source type
    """

    SNOWFLAKE = "snowflake"
    SQLITE = "sqlite"
