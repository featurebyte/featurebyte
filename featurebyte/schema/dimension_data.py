"""
DimensionDataData API payload schema
"""
from __future__ import annotations

from pydantic import StrictStr

from featurebyte.schema.data import DataCreate, DataUpdate


class DimensionDataCreate(DataCreate):
    """
    DimensionData Creation Schema
    """

    dimension_data_primary_key_column: StrictStr


class DimensionDataUpdate(DataUpdate):
    """
    DimensionData Update Schema
    """

    pass
