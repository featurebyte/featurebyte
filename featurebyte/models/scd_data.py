"""
This module contains SCD data related models
"""
from __future__ import annotations

from typing import Any, ClassVar, Type

from pydantic import root_validator

from featurebyte.enum import DBVarType
from featurebyte.models.feature_store import DataModel
from featurebyte.models.validator import construct_data_model_root_validator
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

    # pydantic validators
    _root_validator = root_validator(allow_reuse=True)(
        construct_data_model_root_validator(
            expected_column_field_name_type_pairs=[
                ("record_creation_date_column", DBVarType.supported_timestamp_types()),
                ("effective_timestamp_column", DBVarType.supported_timestamp_types()),
                ("end_timestamp_column", DBVarType.supported_timestamp_types()),
                ("natural_key_column", DBVarType.supported_id_types()),
                ("surrogate_key_column", DBVarType.supported_id_types()),
                ("current_flag_column", None),
            ],
        )
    )

    @root_validator(pre=True)
    @classmethod
    def _handle_current_flag_name(cls, values: dict[str, Any]) -> dict[str, Any]:
        # DEV-556: remove this after migration
        if "current_flag" in values:
            values["current_flag_column"] = values["current_flag"]
        return values
