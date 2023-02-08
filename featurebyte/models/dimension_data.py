"""
This module contains DimensionData related models
"""
from __future__ import annotations

from typing import Any, ClassVar, List, Type

from pydantic import root_validator

from featurebyte.common.validator import construct_data_model_root_validator
from featurebyte.enum import DBVarType
from featurebyte.models.feature_store import DataModel
from featurebyte.query_graph.model.common_table import BaseTableData
from featurebyte.query_graph.model.table import DimensionTableData


class DimensionDataModel(DimensionTableData, DataModel):
    """
    Model for DimensionData entity

    dimension_id_column: str
        The primary key of the dimension data table in the DWH
    """

    _table_data_class: ClassVar[Type[BaseTableData]] = DimensionTableData

    # pydantic validators
    _root_validator = root_validator(allow_reuse=True)(
        construct_data_model_root_validator(
            columns_info_key="columns_info",
            expected_column_field_name_type_pairs=[
                ("record_creation_date_column", DBVarType.supported_timestamp_types()),
                ("dimension_id_column", DBVarType.supported_id_types()),
            ],
        )
    )

    @root_validator(pre=True)
    @classmethod
    def _handle_backward_compatibility(cls, values: dict[str, Any]) -> dict[str, Any]:
        if "dimension_data_id_column" in values:  # DEV-556
            values["dimension_id_column"] = values["dimension_data_id_column"]
        return values

    @property
    def primary_key_columns(self) -> List[str]:
        return [self.dimension_id_column]
