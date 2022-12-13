"""
This module contains SCD data related models
"""
from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import Field, StrictStr, root_validator, validator

from featurebyte.enum import DBVarType, TableDataType
from featurebyte.models.feature_store import DataModel


class SCDDataModel(DataModel):
    """
    Model for Slowly Changing Dimension Type 2 Data entity
    natural_key_column: str
        The column for the natural key (key for which there is one unique active record) in the DWH.
    surrogate_key_column: str
        The column for the surrogate key (the primary key of the SCD) in the DWH.
        The primary key of the dimension data table in the DWH
    effective_timestamp_column: str
        The effective date or timestamp for which the data is valid.
    end_timestamp_column: str
        The end date or timestamp for which the data is valid.
    current_flag: str
        The current status of the data.
    """

    type: Literal[TableDataType.SCD_DATA] = Field(TableDataType.SCD_DATA, const=True)
    natural_key_column: StrictStr
    effective_timestamp_column: StrictStr
    surrogate_key_column: Optional[StrictStr]
    end_timestamp_column: Optional[StrictStr] = Field(default=None)
    current_flag_column: Optional[StrictStr] = Field(default=None)

    @root_validator(pre=True)
    @classmethod
    def _handle_current_flag_name(cls, values: dict[str, Any]) -> dict[str, Any]:
        # DEV-556: remove this after migration
        if "current_flag" in values:
            values["current_flag_column"] = values["current_flag"]
        return values

    @validator("record_creation_date_column", "effective_timestamp_column", "end_timestamp_column")
    @classmethod
    def _check_timestamp_column_exists(
        cls, value: Optional[str], values: dict[str, Any]
    ) -> Optional[str]:
        return DataModel.validate_column_exists(
            column_name=value, values=values, expected_types={DBVarType.TIMESTAMP}
        )

    @validator("natural_key_column", "surrogate_key_column")
    @classmethod
    def _check_id_column_exists(cls, value: Optional[str], values: dict[str, Any]) -> Optional[str]:
        return DataModel.validate_column_exists(
            column_name=value, values=values, expected_types={DBVarType.VARCHAR, DBVarType.INT}
        )

    @validator("current_flag_column")
    @classmethod
    def _check_current_flag_column_exists(
        cls, value: Optional[str], values: dict[str, Any]
    ) -> Optional[str]:
        return DataModel.validate_column_exists(
            column_name=value, values=values, expected_types=None
        )
