"""
This module contains all the enums used across different modules

Note: do not include server only dependencies here
"""

from __future__ import annotations

from typing import Literal

import functools
from enum import Enum

from featurebyte.common.doc_util import FBAutoDoc


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
    The DBVarType enum class provides a way to represent various Database variable types supported by FeatureByte.
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.enum.DBVarType")

    # primitive_types
    BOOL = "BOOL", "Boolean column"
    CHAR = "CHAR", "Fixed-length string column"
    DATE = "DATE", "Date column"
    FLOAT = "FLOAT", "Float / Real column"
    INT = "INT", "Integer column"
    TIME = "TIME", "Time column"
    TIMESTAMP = "TIMESTAMP", "Timestamp column"
    TIMESTAMP_TZ = "TIMESTAMP_TZ", "Timestamp column with timezone offset"
    VARCHAR = "VARCHAR", "Variable-length string column"

    # container types
    ARRAY = "ARRAY", "Array column"
    DICT = "DICT", "Dictionary column"

    # specialized types (extends from either primitive_types or container_types)
    TIMEDELTA = "TIMEDELTA", "Time delta column"
    EMBEDDING = "EMBEDDING", "Embedding column"
    FLAT_DICT = "FLAT_DICT", "Flat dictionary column"

    # unknown type
    UNKNOWN = "UNKNOWN", "Unknown column type"

    # Types to be deprecated (specific for different databases)
    BINARY = "BINARY", "Binary column"
    VOID = "VOID", "Void column"
    MAP = "MAP", "Map column"
    OBJECT = "OBJECT", "Mixed-type column"
    STRUCT = "STRUCT", "Struct column"

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

    @classmethod
    def primitive_types(cls) -> set[DBVarType]:
        """
        List of non-primitive types (not supported by all databases)

        Returns
        -------
        set[DBVarType]
        """
        return {
            cls.INT,
            cls.FLOAT,
            cls.DATE,
            cls.TIME,
            cls.TIMESTAMP,
            cls.TIMESTAMP_TZ,
            cls.BOOL,
            cls.CHAR,
            cls.VARCHAR,
        }

    @classmethod
    def supported_detection_types(cls) -> set[DBVarType]:
        """
        Types for specialized type detection

        Returns
        -------
        set[DBVarType]
        """
        return {cls.ARRAY, cls.OBJECT, cls.STRUCT, cls.DICT}

    @classmethod
    def dictionary_types(cls) -> set[DBVarType]:
        """
        Types for dictionary

        Returns
        -------
        set[DBVarType]
        """
        # FIXME: remove this after we update to the dictionary type
        # Snowflake uses OBJECT for dictionary type & Spark uses STRUCT for dictionary type
        return {cls.OBJECT, cls.STRUCT, cls.DICT, cls.MAP}

    @classmethod
    def array_types(cls) -> set[DBVarType]:
        """
        Types for array

        Returns
        -------
        set[DBVarType]
        """
        return {cls.ARRAY, cls.EMBEDDING}

    @classmethod
    def json_conversion_types(cls) -> set[DBVarType]:
        """
        Types for json conversion

        Returns
        -------
        set[DBVarType]
        """
        return cls.dictionary_types().union({cls.FLAT_DICT}).union(cls.array_types())

    def to_type_str(self) -> str | None:
        """
        Convert DBVarType to internal type string

        Returns
        -------
        str | None
        """
        mapping = {
            self.BOOL: "bool",
            self.VARCHAR: "str",
            self.FLOAT: "float",
            self.INT: "int",
        }
        return mapping.get(self)  # type: ignore


class AggFunc(StrEnum):
    """
    The AggFunc enum class provides a way to represent various aggregation methods in your code. It helps reduce
    errors by defining a set of supported aggregation methods. Each enum constant corresponds to a specific
    aggregation method.

    Examples
    --------
    >>> items_view = catalog.get_view("INVOICEITEMS")
    >>> # Group items by the column GroceryInvoiceGuid that references the customer entity
    >>> items_by_invoice = items_view.groupby("GroceryInvoiceGuid")
    >>> # Get the number of items in each invoice
    >>> invoice_item_count = items_by_invoice.aggregate(  # doctest: +SKIP
    ...   None,
    ...   method=fb.AggFunc.COUNT,
    ...   feature_name="InvoiceItemCount",
    ... )
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.AggFunc")

    SUM = "sum", "Compute sum of values."
    AVG = "avg", "Compute average value."
    MIN = "min", "Compute minimum value."
    MAX = "max", "Compute maximum value."
    COUNT = "count", "Compute row count."
    NA_COUNT = "na_count", "Compute count of missing values."
    STD = "std", "Compute standard deviation of values."
    LATEST = "latest", "Compute the latest value."

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
    The SourceType enum class provides a way to represent different types of data warehouse source in your code.
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.SourceType")

    SNOWFLAKE = "snowflake", "Snowflake connection details."
    SQLITE = "sqlite", "SQLite connection details."
    DATABRICKS = "databricks", "DataBricks connection details."
    DATABRICKS_UNITY = "databricks_unity", "DataBricks Unity Catalog connection details."
    SPARK = "spark", "Spark connection details."

    # TEST source type should only be used for mocking in unit tests.
    TEST = "test", "For testing only."

    @classmethod
    def credential_required_types(cls) -> list[str]:
        """
        List all types that require credential

        Returns
        -------
        list[str]
        """
        return [cls.SNOWFLAKE, cls.DATABRICKS, cls.DATABRICKS_UNITY]


class StorageType(StrEnum):
    """
    The StorageType enum class provides a way to represent different types of distributed storage in your code.
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.StorageType")

    FILE = "file", "Local file storage."
    S3 = "s3", "S3 Storage."
    GCS = "gcs", "Google Cloud Storage."
    AZURE = "azure", "Azure Blob Storage."
    WEBHDFS = "webhdfs", "WebHDFS."


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
    ENTITY_TABLE_START_DATE = "__FB_ENTITY_TABLE_START_DATE"

    LAST_TILE_INDEX = "__FB_LAST_TILE_INDEX"
    FIRST_TILE_INDEX = "__FB_FIRST_TILE_INDEX"

    POINT_IN_TIME_SQL_PLACEHOLDER = "__FB_POINT_IN_TIME_SQL_PLACEHOLDER"

    MIGRATION_VERSION = "MIGRATION_VERSION"
    DATAFRAME_ROW_INDEX = "__FB_DATAFRAME_ROW_INDEX"
    TABLE_ROW_INDEX = "__FB_TABLE_ROW_INDEX"

    ONLINE_STORE_RESULT_NAME_COLUMN = "AGGREGATION_RESULT_NAME"
    ONLINE_STORE_VALUE_COLUMN = "VALUE"
    ONLINE_STORE_VERSION_COLUMN = "VERSION"
    ONLINE_STORE_VERSION_PLACEHOLDER_SUFFIX = "_VERSION_PLACEHOLDER"

    FEATURE_TIMESTAMP_COLUMN = "__feature_timestamp"


class WorkerCommand(StrEnum):
    """
    Command names for worker tasks
    """

    FEATURE_JOB_SETTING_ANALYSIS_CREATE = "FEATURE_JOB_SETTING_ANALYSIS_CREATE"
    FEATURE_JOB_SETTING_ANALYSIS_BACKTEST = "FEATURE_JOB_SETTING_ANALYSIS_BACKTEST"
    HISTORICAL_FEATURE_TABLE_CREATE = "HISTORICAL_TABLE_CREATE"
    OBSERVATION_TABLE_CREATE = "OBSERVATION_TABLE_CREATE"
    OBSERVATION_TABLE_UPLOAD = "OBSERVATION_TABLE_UPLOAD"
    DEPLOYMENT_CREATE_UPDATE = "DEPLOYMENT_CREATE_UPDATE"
    BATCH_REQUEST_TABLE_CREATE = "BATCH_REQUEST_TABLE_CREATE"
    BATCH_FEATURE_TABLE_CREATE = "BATCH_FEATURE_TABLE_CREATE"
    MATERIALIZED_TABLE_DELETE = "MATERIALIZED_TABLE_DELETE"
    BATCH_FEATURE_CREATE = "BATCH_FEATURE_CREATE"
    FEATURE_LIST_CREATE = "FEATURE_LIST_CREATE"
    FEATURE_LIST_MAKE_PRODUCTION_READY = "FEATURE_LIST_MAKE_PRODUCTION_READY"
    STATIC_SOURCE_TABLE_CREATE = "STATIC_SOURCE_TABLE_CREATE"
    TARGET_TABLE_CREATE = "TARGET_TABLE_CREATE"
    TEST = "TEST"
    TILE_COMPUTE = "TILE_COMPUTE"
    ONLINE_STORE_TABLE_CLEANUP = "ONLINE_STORE_TABLE_CLEANUP"
    CATALOG_ONLINE_STORE_UPDATE = "CATALOG_ONLINE_STORE_UPDATE"
    SCHEDULED_FEATURE_MATERIALIZE = "SCHEDULED_FEATURE_MATERIALIZE"
    DATA_DESCRIPTION = "DATA_DESCRIPTION"

    # Tasks to be deprecated
    FEATURE_LIST_CREATE_WITH_BATCH_FEATURE_CREATE = "FEATURE_LIST_CREATE_WITH_BATCH_FEATURE_CREATE"


class TableDataType(StrEnum):
    """
    TableDataType enum
    """

    SOURCE_TABLE = "source_table"
    EVENT_TABLE = "event_table"
    ITEM_TABLE = "item_table"
    DIMENSION_TABLE = "dimension_table"
    SCD_TABLE = "scd_table"


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

    DIMENSION_ID = "dimension_id"
    EVENT_ID = "event_id"
    EVENT_TIMESTAMP = "event_timestamp"
    ITEM_ID = "item_id"
    RECORD_CREATION_TIMESTAMP = "record_creation_timestamp"
    SCD_CURRENT_FLAG = "scd_current_flag"
    SCD_EFFECTIVE_TIMESTAMP = "scd_effective_timestamp"
    SCD_END_TIMESTAMP = "scd_end_timestamp"
    SCD_NATURAL_KEY_ID = "scd_natural_key_id"
    SCD_SURROGATE_KEY_ID = "scd_surrogate_key_id"
    TIME_ZONE = "time_zone"


class MaterializedTableNamePrefix(StrEnum):
    """
    Prefixes for the physical table names of materialized tables
    """

    OBSERVATION_TABLE = "OBSERVATION_TABLE"
    HISTORICAL_FEATURE_TABLE = "HISTORICAL_FEATURE_TABLE"
    BATCH_REQUEST_TABLE = "BATCH_REQUEST_TABLE"
    BATCH_FEATURE_TABLE = "BATCH_FEATURE_TABLE"
    TARGET_TABLE = "TARGET_TABLE"
    FEATURE_TABLE_CACHE = "FEATURE_TABLE_CACHE"

    @classmethod
    def all(cls) -> list[str]:
        """
        List all prefixes

        Returns
        -------
        list[str]
        """
        return [c.value for c in cls]

    @classmethod
    def visible(cls) -> list[str]:
        """
        List all prefixes for visible tables

        Returns
        -------
        list[str]
        """
        return [c.value for c in cls if c.value != cls.FEATURE_TABLE_CACHE]


class FunctionParameterInputForm(StrEnum):
    """Generic function's parameter input form type"""

    VALUE = "value"  # value is used as function argument
    COLUMN = "column"  # column is used as function argument


class UploadFileFormat(StrEnum):
    """
    Upload file format
    """

    CSV = "csv"
    PARQUET = "parquet"


class OnlineStoreType(StrEnum):
    """
    Online store type
    """

    REDIS = "redis"
    MYSQL = "mysql"


class RedisType(str, Enum):
    """
    Redis type
    """

    REDIS = "redis"
    REDIS_CLUSTER = "redis_cluster"


# enum used for handle conflict when saving object to persistent storage
ConflictResolution = Literal["raise", "retrieve"]
