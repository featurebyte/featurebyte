"""
This module contains EventTable related models
"""

from __future__ import annotations

from typing import Any, ClassVar, List, Optional, Tuple, Type

from datetime import datetime

from pydantic import root_validator

from featurebyte.common.validator import construct_data_model_root_validator
from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature_store import TableModel
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.model.table import EventTableData
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ViewMetadata


class FeatureJobSettingHistoryEntry(FeatureByteBaseModel):
    """
    Model for an entry in setting history

    created_at: datetime
        Datetime when the history entry is created
    setting: FeatureJobSetting
        Feature job setting that just becomes history (no longer used) at the time of the history entry creation
    """

    created_at: datetime
    setting: Optional[FeatureJobSetting]


class EventTableModel(EventTableData, TableModel):
    """
    Model for EventTable

    id: PydanticObjectId
        EventTable id of the object
    name : str
        Name of the EventTable
    tabular_source : TabularSource
        Data warehouse connection information & table name tuple
    columns_info: List[ColumnInfo]
        List of event table columns
    event_id_column: str
        Event ID column name
    event_timestamp_column: str
        Event timestamp column name
    default_feature_job_setting : Optional[FeatureJobSetting]
        Default feature job setting
    status : TableStatus
        Status of the EventTable
    created_at : Optional[datetime]
        Datetime when the EventTable was first saved or published
    updated_at: Optional[datetime]
        Datetime when the EventTable object was last updated
    """

    default_feature_job_setting: Optional[FeatureJobSetting]
    _table_data_class: ClassVar[Type[EventTableData]] = EventTableData

    # pydantic validators
    _root_validator = root_validator(allow_reuse=True)(
        construct_data_model_root_validator(
            columns_info_key="columns_info",
            expected_column_field_name_type_pairs=[
                ("event_timestamp_column", DBVarType.supported_timestamp_types()),
                ("record_creation_timestamp_column", DBVarType.supported_timestamp_types()),
                ("event_id_column", DBVarType.supported_id_types()),
                ("event_timestamp_timezone_offset_column", {DBVarType.VARCHAR}),
            ],
        )
    )

    @property
    def primary_key_columns(self) -> List[str]:
        if self.event_id_column:
            return [self.event_id_column]
        return []  # DEV-556: event_id_column should not be empty

    @property
    def special_columns(self) -> List[str]:
        cols = [
            self.event_timestamp_column,
            self.event_id_column,
            self.record_creation_timestamp_column,
            self.event_timestamp_timezone_offset_column,
        ]
        return [col for col in cols if col]

    def create_view_graph_node(
        self, input_node: InputNode, metadata: ViewMetadata, **kwargs: Any
    ) -> Tuple[GraphNode, List[ColumnInfo]]:
        table_data = EventTableData(**self.dict(by_alias=True)).clone(
            column_cleaning_operations=metadata.column_cleaning_operations
        )
        return table_data.construct_event_view_graph_node(  # pylint: disable=no-member
            event_table_node=input_node,
            drop_column_names=metadata.drop_column_names,
            metadata=metadata,
        )
