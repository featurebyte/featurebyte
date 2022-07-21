"""
This module contains DatabaseSource related models
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import Optional, Tuple, Union

from pydantic import StrictStr

from featurebyte.enum import SourceType
from featurebyte.models.base import FeatureByteBaseModel


class SnowflakeDetails(FeatureByteBaseModel):
    """Model for Snowflake data source information"""

    account: StrictStr
    warehouse: StrictStr
    database: StrictStr
    sf_schema: StrictStr  # schema shadows a BaseModel attribute


class SQLiteDetails(FeatureByteBaseModel):
    """Model for SQLite data source information"""

    filename: StrictStr


DatabaseDetails = Union[SnowflakeDetails, SQLiteDetails]


class FeatureStoreModel(FeatureByteBaseModel):
    """Model for a feature store"""

    type: SourceType
    details: DatabaseDetails

    def __hash__(self) -> int:
        """
        Hash function to support use as a dict key

        Returns
        -------
        int
            hash_value
        """
        return hash(str(self.type) + str(self.details))


class TableDetails(FeatureByteBaseModel):
    """Model for table"""

    database_name: Optional[StrictStr]
    schema_name: Optional[StrictStr]
    table_name: StrictStr


class DatabaseTableModel(FeatureByteBaseModel):
    """Model for a database table used in a feature store"""

    tabular_source: Tuple[FeatureStoreModel, TableDetails]
