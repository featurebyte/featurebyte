"""
This module contains all the enums used across different modules
"""
from __future__ import annotations

import functools
from enum import Enum


@functools.total_ordering
class OrderedEnum(Enum):
    """
    OrderedEnum class

    Reference: https://github.com/woodruffw/ordered_enum/blob/master/src/ordered_enum/ordered_enum.py
    """

    @classmethod
    @functools.lru_cache(None)
    def _member_list(cls) -> list[OrderedEnum]:
        return list(cls)

    def __lt__(self, other: object) -> bool:
        if self.__class__ is other.__class__:
            member_list = self.__class__._member_list()
            return member_list.index(self) < member_list.index(other)  # type: ignore
        return NotImplemented

    @classmethod
    def min(cls) -> OrderedEnum:
        """
        Retrieve minimum member of the class

        Returns
        -------
        OrderedEnum
        """
        return min(cls._member_list())

    @classmethod
    def max(cls) -> OrderedEnum:
        """
        Retrieve maximum member of the class

        Returns
        -------
        OrderedEnum
        """
        return max(cls._member_list())


@functools.total_ordering
class OrderedStrEnum(OrderedEnum):
    """
    Ordered String Enum class
    """

    def __eq__(self, other: object) -> bool:
        if isinstance(other, str):
            return super().__eq__(type(self)(other))
        return super().__eq__(other)

    def __lt__(self, other: object) -> bool:
        if isinstance(other, str):
            return super().__lt__(type(self)(other))
        return super().__lt__(other)

    def __hash__(self) -> int:
        return str.__hash__(self.value)

    def __repr__(self) -> str:
        return str(self.value)

    def __str__(self) -> str:
        return str(self.value)


class StrEnum(str, Enum):
    """
    StrEnum class
    """

    def __repr__(self) -> str:
        return str(self.value)

    def __str__(self) -> str:
        return str(self.value)


class DBVarType(StrEnum):
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
    OBJECT = "OBJECT"
    TIMEDELTA = "TIMEDELTA"
    VOID = "VOID"
    ARRAY = "ARRAY"
    MAP = "MAP"
    STRUCT = "STRUCT"


class AggFunc(StrEnum):
    """
    Supported aggregation functions in groupby
    """

    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    NA_COUNT = "na_count"
    STD = "std"

    @classmethod
    def all(cls) -> list[str]:
        """List all defined aggregation function names

        Returns
        -------
        list[str]
        """
        return [c.value for c in cls]


class SourceType(StrEnum):
    """
    Database or data warehouse source type
    """

    SNOWFLAKE = "snowflake"
    SQLITE = "sqlite"
    DATABRICKS = "databricks"


class SpecialColumnName(StrEnum):
    """
    Special column names such as POINT_IN_TIME
    """

    POINT_IN_TIME = "POINT_IN_TIME"


class InternalName(StrEnum):
    """
    Names reserved for featurebyte's internal usage
    """

    TILE_START_DATE = "__FB_TILE_START_DATE_COLUMN"
    TILE_LAST_START_DATE = "LAST_TILE_START_DATE"
    TILE_START_DATE_SQL_PLACEHOLDER = "__FB_START_DATE"
    TILE_END_DATE_SQL_PLACEHOLDER = "__FB_END_DATE"

    TILE_CACHE_WORKING_TABLE = "__FB_TILE_CACHE_WORKING_TABLE"
    TILE_ENTITY_TRACKER_SUFFIX = "_ENTITY_TRACKER"
    LAST_TILE_START_DATE_PREVIOUS = "__FB_LAST_TILE_START_DATE_PREVIOUS"
    ENTITY_TABLE_SQL_PLACEHOLDER = "__FB_ENTITY_TABLE_SQL_PLACEHOLDER"
    ENTITY_TABLE_NAME = "__FB_ENTITY_TABLE_NAME"
    ENTITY_TABLE_START_DATE = "__FB_ENTITY_TABLE_START_DATE"
    ENTITY_TABLE_END_DATE = "__FB_ENTITY_TABLE_END_DATE"

    LAST_TILE_INDEX = "__FB_LAST_TILE_INDEX"
    FIRST_TILE_INDEX = "__FB_FIRST_TILE_INDEX"


class WorkerCommand(StrEnum):
    """
    Command names for worker tasks
    """

    FEATURE_JOB_SETTING_ANALYSIS_CREATE = "FEATURE_JOB_SETTING_ANALYSIS_CREATE"
    FEATURE_JOB_SETTING_ANALYSIS_BACKTEST = "FEATURE_JOB_SETTING_ANALYSIS_BACKTEST"


class TableDataType(StrEnum):
    """
    TableDataType enum
    """

    GENERIC = "generic"
    EVENT_DATA = "event_data"
    ITEM_DATA = "item_data"


class SemanticType(StrEnum):
    """
    Builtin semantic enum
    """

    EVENT_TIMESTAMP = "event_timestamp"
    EVENT_ID = "event_id"
    ITEM_ID = "item_id"
