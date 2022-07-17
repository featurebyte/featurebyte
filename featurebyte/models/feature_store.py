"""
This module contains DatabaseSource related models
"""
from __future__ import annotations

from typing import Optional, Tuple, Union

from pydantic import BaseModel, StrictStr

from featurebyte.enum import SourceType


class SnowflakeDetails(BaseModel):
    """Model for Snowflake data source information"""

    account: StrictStr
    warehouse: StrictStr
    database: StrictStr
    sf_schema: StrictStr  # schema shadows a BaseModel attribute


class SQLiteDetails(BaseModel):
    """Model for SQLite data source information"""

    filename: StrictStr


DatabaseDetails = Union[SnowflakeDetails, SQLiteDetails]


class FeatureStoreModel(BaseModel):
    """Model for a feature store"""

    type: SourceType
    details: DatabaseDetails

    class Config:
        """
        Configuration for FeatureStoreModel
        """

        use_enum_values = True

    def __hash__(self) -> int:
        """
        Hash function to support use as a dict key

        Returns
        -------
        int
            hash_value
        """
        return hash(str(self.type) + str(self.details))


class TableDetails(BaseModel):
    """Model for table"""

    database_name: Optional[StrictStr]
    schema_name: Optional[StrictStr]
    table_name: StrictStr


class DatabaseTableModel(BaseModel):
    """Model for a database table used in a feature store"""

    tabular_source: Tuple[FeatureStoreModel, TableDetails]
