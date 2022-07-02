"""
This module contains DatabaseSource related models
"""
from __future__ import annotations

from typing import Optional, Tuple, Union

from pydantic import BaseModel

from featurebyte.enum import SourceType


class SnowflakeDetails(BaseModel):
    """Model for Snowflake data source information"""

    account: str
    warehouse: str
    database: str
    sf_schema: str  # schema shadows a BaseModel attribute


class SQLiteDetails(BaseModel):
    """Model for SQLite data source information"""

    filename: str


DatabaseDetails = Union[SnowflakeDetails, SQLiteDetails]


class DatabaseSourceModel(BaseModel):
    """Model for a database source"""

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


class TableDetails(BaseModel):
    """Model for table"""

    database_name: Optional[str]
    schema_name: Optional[str]
    table_name: str


class DatabaseTableModel(BaseModel):
    """Model for a table of database source"""

    tabular_source: Tuple[DatabaseSourceModel, TableDetails]
