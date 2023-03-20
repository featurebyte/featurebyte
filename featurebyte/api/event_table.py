"""
EventTable class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, List, Literal, Optional, Type, Union, cast

from datetime import datetime

import pandas as pd
from pydantic import Field, StrictStr, root_validator
from typeguard import typechecked

from featurebyte.api.base_table import TableApiObject
from featurebyte.api.feature_job_setting_analysis import FeatureJobSettingAnalysis
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.validator import construct_data_model_root_validator
from featurebyte.enum import DBVarType, TableDataType, ViewMode
from featurebyte.exception import InvalidSettingsError, RecordRetrievalException
from featurebyte.models.event_table import EventTableModel
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.model.table import AllTableDataT, EventTableData
from featurebyte.query_graph.node.cleaning_operation import ColumnCleaningOperation
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ViewMetadata
from featurebyte.schema.event_table import EventTableCreate, EventTableUpdate
from featurebyte.schema.feature_job_setting_analysis import FeatureJobSettingAnalysisCreate

if TYPE_CHECKING:
    from featurebyte.api.event_view import EventView


class EventTable(TableApiObject):
    """
    EventTable is an object connected with an event table in the data warehouse. These tables must have the following
    properties:\n
    - and an event_id column as a primary key\n
    - an event timestamp

    Users are strongly encouraged to annotate the table by tagging entities and defining:

    - the semantic of the table field
    - critical data information on the data quality that requires cleaning before feature engineering.

    Before registering a new EventTable, users are asked to set the default for the FeatureJob scheduling for features
    that will be extracted from the EventTable.

    To build features, users create Event Views from EventTable.
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Table"], proxy_class="featurebyte.EventTable")

    # class variables
    _route = "/event_table"
    _update_schema_class = EventTableUpdate
    _create_schema_class = EventTableCreate
    _get_schema = EventTableModel
    _table_data_class: ClassVar[Type[AllTableDataT]] = EventTableData

    # pydantic instance variable (public)
    type: Literal[TableDataType.EVENT_TABLE] = Field(TableDataType.EVENT_TABLE, const=True)

    # pydantic instance variable (internal use)
    internal_default_feature_job_setting: Optional[FeatureJobSetting] = Field(
        alias="default_feature_job_setting"
    )
    internal_event_timestamp_column: StrictStr = Field(alias="event_timestamp_column")
    internal_event_id_column: Optional[StrictStr] = Field(alias="event_id_column")  # DEV-556

    # pydantic validators
    _root_validator = root_validator(allow_reuse=True)(
        construct_data_model_root_validator(
            columns_info_key="internal_columns_info",
            expected_column_field_name_type_pairs=[
                (
                    "internal_record_creation_timestamp_column",
                    DBVarType.supported_timestamp_types(),
                ),
                ("internal_event_timestamp_column", DBVarType.supported_timestamp_types()),
                ("internal_event_id_column", DBVarType.supported_id_types()),
            ],
        )
    )

    def get_view(
        self,
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL] = ViewMode.AUTO,
        drop_column_names: Optional[List[str]] = None,
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]] = None,
    ) -> EventView:
        """
        Construct an EventView object

        Parameters
        ----------
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL]
            View mode to use (manual or auto), when auto, the view will be constructed with cleaning operations
            from the table and the record creation timestamp column will be dropped
        drop_column_names: Optional[List[str]]
            List of column names to drop (manual mode only)
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]]
            Column cleaning operations to apply (manual mode only)

        Returns
        -------
        EventView
            constructed EventView object
        """
        from featurebyte.api.event_view import EventView  # pylint: disable=import-outside-toplevel

        self._validate_view_mode_params(
            view_mode=view_mode,
            drop_column_names=drop_column_names,
            column_cleaning_operations=column_cleaning_operations,
        )

        # The input of view graph node is the table node. The final graph looks like this:
        #    +-----------+     +----------------------------+
        #    | InputNode + --> | GraphNode(type:event_view) +
        #    +-----------+     +----------------------------+
        drop_column_names = drop_column_names or []
        if view_mode == ViewMode.AUTO and self.record_creation_timestamp_column:
            drop_column_names.append(self.record_creation_timestamp_column)

        data_node = self.frame.node
        assert isinstance(data_node, InputNode)
        event_table_data = cast(EventTableData, self.table_data)
        (
            event_table_data,
            column_cleaning_operations,
        ) = self._prepare_table_data_and_column_cleaning_operations(
            table_data=event_table_data,
            column_cleaning_operations=column_cleaning_operations,
            view_mode=view_mode,
        )

        view_graph_node, columns_info = event_table_data.construct_event_view_graph_node(
            event_table_node=data_node,
            drop_column_names=drop_column_names,
            metadata=ViewMetadata(
                view_mode=view_mode,
                drop_column_names=drop_column_names,
                column_cleaning_operations=column_cleaning_operations,
                table_id=data_node.parameters.id,
            ),
        )
        inserted_graph_node = GlobalQueryGraph().add_node(view_graph_node, input_nodes=[data_node])
        return EventView(
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            columns_info=columns_info,
            node_name=inserted_graph_node.name,
            tabular_data_ids=[self.id],
            default_feature_job_setting=self.default_feature_job_setting,
            event_id_column=self.event_id_column,
        )

    @property
    def default_feature_job_setting(self) -> Optional[FeatureJobSetting]:
        """
        Default feature job setting of the EventTable

        Returns
        -------
        Optional[FeatureJobSetting]
        """
        try:
            return self.cached_model.default_feature_job_setting
        except RecordRetrievalException:
            return self.internal_default_feature_job_setting

    @property
    def event_timestamp_column(self) -> str:
        """
        Event timestamp column name of the EventTable

        Returns
        -------
        str
        """
        try:
            return self.cached_model.event_timestamp_column
        except RecordRetrievalException:
            return self.internal_event_timestamp_column

    @property
    def event_id_column(self) -> Optional[str]:
        """
        Event ID column name of the EventTable associated with the ItemTable

        Returns
        -------
        str
        """
        try:
            return self.cached_model.event_id_column
        except RecordRetrievalException:
            return self.internal_event_id_column

    @property
    def timestamp_column(self) -> Optional[str]:
        """
        Timestamp column name of the EventTable

        Returns
        -------
        Optional[str]
        """
        return self.event_timestamp_column

    @property
    def default_feature_job_setting_history(self) -> list[dict[str, Any]]:
        """
        List of default_job_setting history entries

        Returns
        -------
        list[dict[str, Any]]
        """
        return self._get_audit_history(field_name="default_feature_job_setting")

    @typechecked
    def update_default_feature_job_setting(self, feature_job_setting: FeatureJobSetting) -> None:
        """
        Update default feature job setting

        Parameters
        ----------
        feature_job_setting: FeatureJobSetting
            Feature job setting object
        """
        self.update(
            update_payload={"default_feature_job_setting": feature_job_setting.dict()},
            allow_update_local=True,
            add_internal_prefix=True,
        )

    @typechecked
    def create_new_feature_job_setting_analysis(
        self,
        analysis_date: Optional[datetime] = None,
        analysis_length: int = 2419200,
        min_featurejob_period: int = 60,
        exclude_late_job: bool = False,
        blind_spot_buffer_setting: int = 5,
        job_time_buffer_setting: Union[int, Literal["auto"]] = "auto",
        late_data_allowance: float = 5e-5,
    ) -> FeatureJobSettingAnalysis:
        """
        Create new feature job setting analysis on the event table

        Parameters
        ----------
        analysis_date: Optional[datetime]
            Analysis date
        analysis_length: int
            Length of analysis (seconds)
        min_featurejob_period: int
            Minimum period for a feature job
        exclude_late_job: bool
            Exclude late jobs in analysis
        job_time_buffer_setting: Union[int, Literal["auto"]]
            Buffer time for job execution (seconds)
        blind_spot_buffer_setting: int
            Buffer time for table population blind spot
        late_data_allowance: float
            Threshold for late records (percentile)

        Returns
        -------
        FeatureJobSettingAnalysis
        """
        payload = FeatureJobSettingAnalysisCreate(
            event_table_id=self.id,
            analysis_date=analysis_date,
            analysis_length=analysis_length,
            min_featurejob_period=min_featurejob_period,
            exclude_late_job=exclude_late_job,
            blind_spot_buffer_setting=blind_spot_buffer_setting,
            job_time_buffer_setting=job_time_buffer_setting,
            late_data_allowance=late_data_allowance,
        )
        job_setting_analysis = self.post_async_task(
            route="/feature_job_setting_analysis", payload=payload.json_dict()
        )
        analysis = FeatureJobSettingAnalysis.get_by_id(job_setting_analysis["_id"])
        analysis.display_report()
        return analysis

    @typechecked
    def initialize_default_feature_job_setting(self) -> None:
        """
        Initialize default feature job setting by performing an analysis on the table

        Raises
        ------
        InvalidSettingsError
            Default feature job setting is already initialized
        """
        if self.default_feature_job_setting:
            raise InvalidSettingsError("Default feature job setting is already initialized")

        analysis = self.create_new_feature_job_setting_analysis()
        self.update_default_feature_job_setting(analysis.get_recommendation())

    @typechecked
    def list_feature_job_setting_analysis(self) -> Optional[pd.DataFrame]:
        """
        List feature job setting analysis that has been performed

        Returns
        -------
        Optional[DataFrame]
            Table of feature job analysis
        """
        return FeatureJobSettingAnalysis.list(event_table_id=self.id)
