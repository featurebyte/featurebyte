"""
This module contains SCD data related models
"""
from __future__ import annotations

from typing import Any, ClassVar, Optional, Type

from pydantic import root_validator, validator

from featurebyte.enum import DBVarType
from featurebyte.models.feature_store import DataModel
from featurebyte.query_graph.model.common_table import BaseTableData
from featurebyte.query_graph.model.table import SCDTableData


class SCDDataModel(SCDTableData, DataModel):
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

    _table_data_class: ClassVar[Type[BaseTableData]] = SCDTableData

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
            column_name=value,
            values=values,
            expected_types={DBVarType.TIMESTAMP, DBVarType.TIMESTAMP_TZ},
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
