"""
This module contains DimensionData related models
"""
from __future__ import annotations

from pydantic import Field, StrictStr

from featurebyte.enum import TableDataType
from featurebyte.models.feature_store import DataModel


class DimensionDataModel(DataModel):
    """
    Model for DimensionData entity

    dimension_data_primary_key_column: str
        The primary key of the dimension data table in the DWH
    """

    type: TableDataType = Field(TableDataType.DIMENSION_DATA, const=True)
    dimension_data_primary_key_column: StrictStr = Field(default=None)
