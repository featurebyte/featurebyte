"""
This module contains feature store related models that are used in featurebyte/query_graph.
"""
from __future__ import annotations

from typing import ClassVar, Union

from pydantic import StrictStr

from featurebyte.enum import SourceType
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


class TestDatabaseDetails(BaseDatabaseDetails):
    """Model for a no-op mock database details for use in tests"""


DatabaseDetails = Union[SnowflakeDetails, SQLiteDetails, DatabricksDetails, TestDatabaseDetails]


class FeatureStoreDetails(FeatureByteBaseModel):
    """FeatureStoreDetail"""

    type: SourceType
    details: DatabaseDetails
