"""
This module contains DimensionData related models
"""
from __future__ import annotations

from typing import Any, ClassVar, Optional, Type

from pydantic import validator

from featurebyte.enum import DBVarType
from featurebyte.models.feature_store import DataModel
from featurebyte.query_graph.model.common_table import BaseTableData
from featurebyte.query_graph.model.table import DimensionTableData


class DimensionDataModel(DimensionTableData, DataModel):
    """
    Model for DimensionData entity

    dimension_data_id_column: str
        The primary key of the dimension data table in the DWH
    """

    _table_data_class: ClassVar[Type[BaseTableData]] = DimensionTableData

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
