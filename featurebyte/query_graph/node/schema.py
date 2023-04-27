"""
This module contains feature store & table schemas that are used in node parameters.
"""
from __future__ import annotations

from typing import ClassVar, Optional, Union

from pydantic import Field, StrictStr

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import DBVarType, SourceType, StorageType
from featurebyte.models.base import FeatureByteBaseModel


class BaseDatabaseDetails(FeatureByteBaseModel):
    """Model for data source information"""

    is_local_source: ClassVar[bool] = False


class SnowflakeDetails(BaseDatabaseDetails):
    """Model for Snowflake data source information."""

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.SnowflakeDetails")

    account: StrictStr = Field(
        description="The host account name which can be found using one of the following "
        "formats:\n"
        "- For Amazon Web Services US West, use`<account>.snowflakecomputing.com`\n"
        "- For all other regions on Amazon Web Services, use `<account>.<region>.snowflakecomputing.com`\n"
        "- For all regions on Microsoft Azure, use `<account>.<region>.azure.snowflakecomputing.com`"
    )
    warehouse: StrictStr = Field(
        description="The name of the warehouse containing the schema tables and columns."
    )
    database: StrictStr = Field(
        description="The name of the database containing the schema tables and columns."
    )
    sf_schema: StrictStr = Field(
        description="The name of the schema containing the database, tables and columns."
    )


class SQLiteDetails(BaseDatabaseDetails):
    """Model for SQLite data source information"""

    filename: StrictStr
    is_local_source: ClassVar[bool] = True


class DatabricksDetails(BaseDatabaseDetails):
    """Model for Databricks data source information"""

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.DatabricksDetails")

    host: StrictStr
    http_path: StrictStr
    featurebyte_catalog: StrictStr
    featurebyte_schema: StrictStr
    storage_type: StorageType = Field(
        description="Storage type of where we will be persisting the feature store to."
    )
    storage_url: str = Field(
        description="Storage URL of where we will be persisting the feature store to."
    )
    storage_spark_url: str = Field(
        description="Spark URL of where we will be persisting the feature store to."
    )


class SparkDetails(BaseDatabaseDetails):
    """Model for Spark data source information"""

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.SparkDetails")

    host: StrictStr = Field(default="127.0.0.1")
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

    type: SourceType = Field(
        description="Returns the type of the feature store (Spark, Snowflake, DataBricks,...) "
        "represented by the FeatureStore object."
    )
    details: DatabaseDetails = Field(
        description="Returns the details of the database used for the FeatureStore " "object."
    )


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
