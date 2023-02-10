"""
This module contains feature store & table schemas that are used in node parameters.
"""
from __future__ import annotations

from typing import ClassVar, Optional, Union

from pydantic import Field, StrictStr

from featurebyte.enum import DBVarType, SourceType, StorageType
from featurebyte.models.base import FeatureByteBaseModel


class BaseDatabaseDetails(FeatureByteBaseModel):
    """Model for data source information"""

    is_local_source: ClassVar[bool] = False


class SnowflakeDetails(BaseDatabaseDetails):
    """Model for Snowflake data source information"""

    account: StrictStr
    warehouse: StrictStr
    database: StrictStr
    sf_schema: StrictStr  # schema shadows a BaseModel attribute


class SQLiteDetails(BaseDatabaseDetails):
    """Model for SQLite data source information"""

    filename: StrictStr
    is_local_source: ClassVar[bool] = True


class DatabricksDetails(BaseDatabaseDetails):
    """Model for Databricks data source information"""

    server_hostname: StrictStr
    http_path: StrictStr
    featurebyte_catalog: StrictStr
    featurebyte_schema: StrictStr


class SparkDetails(BaseDatabaseDetails):
    """Model for Spark data source information"""

    host: StrictStr = Field(default="localhost")
    port: int = Field(default=10000)
    http_path: StrictStr = Field(default="cliservice")
    use_http_transport: bool = Field(default=False)
    use_ssl: bool = Field(default=False)
    storage_type: StorageType
    storage_url: str
    storage_spark_url: StrictStr
    featurebyte_catalog: StrictStr
    featurebyte_schema: StrictStr


class TestDatabaseDetails(BaseDatabaseDetails):
    """Model for a no-op mock database details for use in tests"""


DatabaseDetails = Union[
    SnowflakeDetails, SparkDetails, SQLiteDetails, DatabricksDetails, TestDatabaseDetails
]


class FeatureStoreDetails(FeatureByteBaseModel):
    """FeatureStoreDetail"""

    type: SourceType
    details: DatabaseDetails


class TableDetails(FeatureByteBaseModel):
    """Table details"""

    database_name: Optional[StrictStr]
    schema_name: Optional[StrictStr]
    table_name: StrictStr


class ColumnSpec(FeatureByteBaseModel):
    """
    Schema for columns retrieval
    """

    name: StrictStr
    dtype: DBVarType
