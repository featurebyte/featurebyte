"""
This module contains DatabaseSource related models
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import List, Optional, Tuple, Union

from beanie import PydanticObjectId
from pydantic import StrictStr

from featurebyte.enum import SourceType
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    UniqueValuesConstraint,
)


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

    class Settings:
        """
        MongoDB settings
        """

        collection_name: str = "feature_store"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature="get",
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature="get",
            ),
            UniqueValuesConstraint(
                fields=("details",),
                conflict_fields_signature={"details": ["details"]},
                resolution_signature="get",
            ),
        ]


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
