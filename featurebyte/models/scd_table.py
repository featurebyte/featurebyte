"""
This module contains SCD table related models
"""

from __future__ import annotations

from typing import Any, ClassVar, List, Optional, Tuple, Type, Union

from pydantic import BaseModel, Field, model_validator

from featurebyte.common.validator import ColumnToTimestampSchema, construct_data_model_validator
from featurebyte.enum import DBVarType
from featurebyte.models.feature_store import TableModel
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.model.table import SCDTableData
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ChangeViewMetadata, ViewMetadata


class SCDTableModel(SCDTableData, TableModel):
    """
    Model for Slowly Changing Dimension Type 2 Data entity

    default_feature_job_setting : Optional[FeatureJobSetting]
        Default feature job setting
    natural_key_column: Optional[str]
        The column for the natural key (key for which there is one unique active record) in the DWH.
    surrogate_key_column: str
        The column for the surrogate key (the primary key of the SCD) in the DWH.
        The primary key of the dimension table in the DWH
    effective_timestamp_column: str
        The effective date or timestamp for which the table is valid.
    end_timestamp_column: str
        The end date or timestamp for which the table is valid.
    current_flag: str
        The current status of the table.
    """

    default_feature_job_setting: Optional[FeatureJobSetting] = Field(default=None)
    _table_data_class: ClassVar[Type[SCDTableData]] = SCDTableData

    # pydantic validators
    _model_validator = model_validator(mode="after")(
        construct_data_model_validator(
            columns_info_key="columns_info",
            expected_column_field_name_type_pairs=[
                ("record_creation_timestamp_column", DBVarType.supported_timestamp_types()),
                ("effective_timestamp_column", DBVarType.supported_datetime_types()),
                ("end_timestamp_column", DBVarType.supported_datetime_types()),
                ("natural_key_column", DBVarType.supported_id_types()),
                ("surrogate_key_column", DBVarType.supported_id_types()),
                ("current_flag_column", None),
            ],
            column_to_timestamp_schema_pairs=[
                ColumnToTimestampSchema("effective_timestamp_column", "effective_timestamp_schema"),
                ColumnToTimestampSchema("end_timestamp_column", "end_timestamp_schema"),
            ],
        )
    )

    @model_validator(mode="before")
    @classmethod
    def _handle_current_flag_name(cls, values: Any) -> Any:
        # DEV-556: remove this after migration
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

        if "current_flag" in values:
            values["current_flag_column"] = values["current_flag"]
        return values

    @property
    def primary_key_columns(self) -> List[str]:
        return [self.natural_key_column] if self.natural_key_column else []

    @property
    def special_columns(self) -> List[str]:
        cols = [
            self.natural_key_column,
            self.surrogate_key_column,
            self.effective_timestamp_column,
            self.end_timestamp_column,
            self.current_flag_column,
            self.record_creation_timestamp_column,
        ]
        return [col for col in cols if col]

    def create_view_graph_node(
        self,
        input_node: InputNode,
        metadata: Union[ViewMetadata, ChangeViewMetadata],
        **kwargs: Any,
    ) -> Tuple[GraphNode, List[ColumnInfo]]:
        table_data = SCDTableData(**self.model_dump(by_alias=True)).clone(
            column_cleaning_operations=metadata.column_cleaning_operations,
        )
        if isinstance(metadata, ChangeViewMetadata):
            return table_data.construct_change_view_graph_node(
                scd_table_node=input_node,
                track_changes_column=metadata.track_changes_column,
                prefixes=metadata.prefixes,
                drop_column_names=metadata.drop_column_names,
                metadata=metadata,
            )

        return table_data.construct_scd_view_graph_node(
            scd_table_node=input_node,
            drop_column_names=metadata.drop_column_names,
            metadata=metadata,
        )
