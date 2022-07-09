"""
This module contains all the enums used across different modules
"""
from __future__ import annotations

from enum import Enum


class CollectionName(str, Enum):
    """
    Collection name
    """

    EVENT_DATA = "event_data"
    ENTITY = "entity"


class DBVarType(str, Enum):
    """
    Database variable type
    """

    BINARY = "BINARY"
    BOOL = "BOOL"
    CHAR = "CHAR"
    DATE = "DATE"
    FLOAT = "FLOAT"
    INT = "INT"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    VARCHAR = "VARCHAR"


class AggFunc(str, Enum):
    """
    Supported aggregation functions in groupby
    """

    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    NA_COUNT = "na_count"

    @classmethod
    def all(cls) -> list[str]:
        """List all defined aggregation function names

        Returns
        -------
        list[str]
        """
        return [c.value for c in cls]


class SourceType(str, Enum):
    """
    Database or data warehouse source type
    """

    SNOWFLAKE = "snowflake"
    SQLITE = "sqlite"


class SpecialColumnName(str, Enum):
    """
    Special column names such as POINT_IN_TIME
    """

    POINT_IN_TIME = "POINT_IN_TIME"


class InternalName(str, Enum):
    """
    Names reserved for featurebyte's internal usage
    """

    TILE_START_DATE = "__FB_TILE_START_DATE_COLUMN"
    TILE_START_DATE_SQL_PLACEHOLDER = "__FB_START_DATE"
    TILE_END_DATE_SQL_PLACEHOLDER = "__FB_END_DATE"

    TILE_ENTITY_TRACKER_SUFFIX = "_ENTITY_TRACKER"
    LAST_TILE_START_DATE = "__FB_LAST_TILE_START_DATE"
    LAST_TILE_START_DATE_PREVIOUS = "__FB_LAST_TILE_START_DATE_PREVIOUS"
    ENTITY_TABLE_SQL_PLACEHOLDER = "__FB_ENTITY_TABLE_NAME"
    ENTITY_TABLE_START_DATE = "__FB_ENTITY_TABLE_START_DATE"
    ENTITY_TABLE_END_DATE = "__FB_ENTITY_TABLE_END_DATE"
