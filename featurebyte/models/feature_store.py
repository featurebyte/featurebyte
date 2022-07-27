"""
This module contains DatabaseSource related models
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import Optional, Tuple, Union

from beanie import PydanticObjectId
from pydantic import StrictStr

from featurebyte.enum import SourceType
from featurebyte.models.base import FeatureByteBaseDocumentModel, FeatureByteBaseModel


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


class FeatureStoreModel(FeatureByteBaseDocumentModel):
    """Model for a feature store"""

    name: StrictStr
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


FeatureStoreIdentifier = PydanticObjectId
TabularSource = Tuple[FeatureStoreIdentifier, TableDetails]


class DatabaseTableModel(FeatureByteBaseModel):
    """Model for a database table used in a feature store"""

    tabular_source: TabularSource
