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

    def __new__(cls, value: str, doc: str | None = None) -> StrEnum:
        """
        Support inline documentation of enum values

        Parameters
        ----------
        value: str
            Value of enum member
        doc: str
            Docstring for enum member

        Returns
        -------
        StrEnum
        """
        self = str.__new__(cls, value)
        self._value_ = value
        if doc is not None:
            self.__doc__ = doc
        return self

    def __repr__(self) -> str:
        return str(self.value)

    def __str__(self) -> str:
        return str(self.value)


class DBVarType(StrEnum):
    """
    Database variable type
    """

    BINARY = "BINARY", "Binary column"
    BOOL = "BOOL", "Boolean column"
    CHAR = "CHAR", "Fixed-length string column"
    DATE = "DATE", "Date column"
    FLOAT = "FLOAT", "Float / Real column"
    INT = "INT", "Integer column"
    TIME = "TIME", "Time column"
    TIMESTAMP = "TIMESTAMP", "Timestamp column"
    TIMESTAMP_TZ = "TIMESTAMP_TZ", "Timestamp column with timezone offset"
    VARCHAR = "VARCHAR", "Variable-length string column"
    OBJECT = "OBJECT", "Mixed-type column"
    TIMEDELTA = "TIMEDELTA", "Time delta column"
    VOID = "VOID", "Void column"
    ARRAY = "ARRAY", "Array column"
    MAP = "MAP", "Map column"
    STRUCT = "STRUCT", "Struct column"
    UNKNOWN = "UNKNOWN", "Unknown column type"

    @classmethod
    def supported_timestamp_types(cls) -> set[DBVarType]:
        """
        Supported timestamp types

        Returns
        -------
        set[DBVarType]
        """
        return {cls.TIMESTAMP, cls.TIMESTAMP_TZ}

    @classmethod
    def supported_id_types(cls) -> set[DBVarType]:
        """
        Supported id column types

        Returns
        -------
        set[DBVarType]
        """
        return {cls.VARCHAR, cls.INT}

    def to_type_str(self) -> str | None:
        """
        Convert DBVarType to internal type string

        Returns
        -------
        str | None
        """
        mapping = {
            self.BOOL: "bool",
            self.CHAR: "str",
            self.VARCHAR: "str",
            self.FLOAT: "float",
            self.INT: "int",
        }
        return mapping.get(self)  # type: ignore


class AggFunc(StrEnum):
    """
    Supported aggregation functions in groupby
    """

    __fbautodoc_proxy_class__: tuple[str, str] = ("featurebyte.AggFunc", "")

    SUM = "sum", "Compute sum of values"
    AVG = "avg", "Compute average value"
    MIN = "min", "Compute minimum value"
    MAX = "max", "Compute maximum value"
    COUNT = "count", "Compute row count"
    NA_COUNT = "na_count", "Compute count of missing values"
    STD = "std", "Compute standard deviation of values"
    LATEST = "latest", "Compute the latest value"

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

    __fbautodoc_proxy_class__: tuple[str, str] = ("featurebyte.SourceType", "")

    SNOWFLAKE = "snowflake", "Snowflake connection details"
    SQLITE = "sqlite", "SQLite connection details"
    DATABRICKS = "databricks", "DataBricks connection details"
    SPARK = "spark", "Spark connection details"

    # TEST source type should only be used for mocking in unit tests.
    TEST = "test", "For testing only"

    @classmethod
    def credential_required_types(cls) -> list[str]:
        """
        List all types that require credential

        Returns
        -------
        list[str]
        """
        return [cls.SNOWFLAKE, cls.DATABRICKS]


class StorageType(StrEnum):
    """
    Distributed storage type
    """

    FILE = "file"
    S3 = "s3", "s3 Storage"


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
    ENTITY_TABLE_END_DATE = "__FB_ENTITY_TABLE_END_DATE"

    LAST_TILE_INDEX = "__FB_LAST_TILE_INDEX"
    FIRST_TILE_INDEX = "__FB_FIRST_TILE_INDEX"

    POINT_IN_TIME_SQL_PLACEHOLDER = "__FB_POINT_IN_TIME_SQL_PLACEHOLDER"

    MIGRATION_VERSION = "MIGRATION_VERSION"
    ROW_INDEX = "__FB_ROW_INDEX"


class WorkerCommand(StrEnum):
    """
    Command names for worker tasks
    """

    TEST = "TEST_TASK"
    FEATURE_JOB_SETTING_ANALYSIS_CREATE = "FEATURE_JOB_SETTING_ANALYSIS_CREATE"
    FEATURE_JOB_SETTING_ANALYSIS_BACKTEST = "FEATURE_JOB_SETTING_ANALYSIS_BACKTEST"
    TILE = "TILE_TASK"


class TableDataType(StrEnum):
    """
    TableDataType enum
    """

    GENERIC = "generic"
    EVENT_DATA = "event_data"
    ITEM_DATA = "item_data"
    DIMENSION_DATA = "dimension_data"
    SCD_DATA = "scd_data"


class ViewMode(StrEnum):
    """
    ViewMode enum
    """

    AUTO = "auto"
    MANUAL = "manual"


class SemanticType(StrEnum):
    """
    Builtin semantic enum
    """

    EVENT_TIMESTAMP = "event_timestamp"
    EVENT_ID = "event_id"
    ITEM_ID = "item_id"
    DIMENSION_ID = "dimension_id"
    SCD_NATURAL_KEY_ID = "scd_natural_key_id"
    SCD_SURROGATE_KEY_ID = "scd_surrogate_key_id"
