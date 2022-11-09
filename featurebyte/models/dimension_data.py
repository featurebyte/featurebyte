"""
This module contains DimensionData related models
"""
from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import Field, StrictStr, validator

from featurebyte.enum import DBVarType, TableDataType
from featurebyte.models.feature_store import DataModel


class DimensionDataModel(DataModel):
    """
    Model for DimensionData entity

    dimension_data_id_column: str
        The primary key of the dimension data table in the DWH
    """

    type: Literal[TableDataType.DIMENSION_DATA] = Field(TableDataType.DIMENSION_DATA, const=True)
    dimension_data_id_column: StrictStr

    @validator("record_creation_date_column")
    @classmethod
    def _check_timestamp_column_exists(
        cls, value: Optional[str], values: dict[str, Any]
    ) -> Optional[str]:
        return DataModel.validate_column_exists(
            column_name=value, values=values, expected_types={DBVarType.TIMESTAMP}
        )

    @validator("dimension_data_id_column")
    @classmethod
    def _check_id_column_exists(cls, value: Optional[str], values: dict[str, Any]) -> Optional[str]:
        return DataModel.validate_column_exists(
            column_name=value, values=values, expected_types={DBVarType.VARCHAR, DBVarType.INT}
        )
