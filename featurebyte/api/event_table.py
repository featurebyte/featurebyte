"""
EventTable class
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar, List, Optional, Type, Union, cast

import pandas as pd
from bson import ObjectId
from pydantic import Field, StrictStr, model_validator
from typeguard import typechecked
from typing_extensions import Literal

from featurebyte.api.base_table import TableApiObject
from featurebyte.api.feature_job_setting_analysis import FeatureJobSettingAnalysis
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.validator import construct_data_model_validator
from featurebyte.enum import DBVarType, TableDataType, ViewMode
from featurebyte.exception import InvalidSettingsError, RecordRetrievalException
from featurebyte.models.event_table import EventTableModel
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.feature_job_setting import (
    FeatureJobSettingUnion,
)
from featurebyte.query_graph.model.table import AllTableDataT, EventTableData
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.node.cleaning_operation import ColumnCleaningOperation
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ViewMetadata
from featurebyte.schema.event_table import EventTableCreate, EventTableUpdate
from featurebyte.schema.feature_job_setting_analysis import FeatureJobSettingAnalysisCreate

if TYPE_CHECKING:
    from featurebyte.api.event_view import EventView


class EventTable(TableApiObject):
    """
    An EventTable object represents a source table where each row indicates a specific business event measured at a
    particular moment.

    Event tables can take various forms, such as an Order table in E-commerce, Credit Card Transactions in Banking,
    Doctor Visits in Healthcare, and Clickstream on the Internet.

    EventTable objects are created from a SourceTable object via the create_event_table method, and by identifying
    the columns representing the event key and timestamp. Additionally, the column that represents the record
    creation timestamp may be identified to enable an automatic analysis of data availability and freshness of the
    source table. This analysis can assist in selecting the default scheduling of the computation of features
    associated with the Event table (default FeatureJob setting).

    After creation, the table can optionally incorporate additional metadata at the column level to further aid feature
    engineering. This can include identifying columns that identify or reference entities, providing information about
    the semantics of the table columns, specifying default cleaning operations, or furnishing descriptions of its
    columns.

    See Also
    --------
    - [create_event_table](/reference/featurebyte.api.source_table.SourceTable.create_event_table/): create event table from source table
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.EventTable",
        skip_params_and_signature_in_class_docs=True,
    )
    _route: ClassVar[str] = "/event_table"
    _update_schema_class: ClassVar[Any] = EventTableUpdate
    _create_schema_class: ClassVar[Any] = EventTableCreate
    _get_schema: ClassVar[Any] = EventTableModel
    _table_data_class: ClassVar[Type[AllTableDataT]] = EventTableData

    # pydantic instance variable (public)
    type: Literal[TableDataType.EVENT_TABLE] = TableDataType.EVENT_TABLE

    # pydantic instance variable (internal use)
    internal_default_feature_job_setting: Optional[FeatureJobSettingUnion] = Field(
        alias="default_feature_job_setting", default=None
    )
    internal_event_timestamp_column: StrictStr = Field(alias="event_timestamp_column")
    internal_event_id_column: Optional[StrictStr] = Field(
        alias="event_id_column", default=None
    )  # DEV-556
    internal_event_timestamp_timezone_offset: Optional[StrictStr] = Field(
        alias="event_timestamp_timezone_offset", default=None
    )
    internal_event_timestamp_timezone_offset_column: Optional[StrictStr] = Field(
        alias="event_timestamp_timezone_offset_column", default=None
    )
    internal_event_timestamp_schema: Optional[TimestampSchema] = Field(
        alias="event_timestamp_schema",
        default=None,
    )

    # pydantic validators
    _model_validator = model_validator(mode="after")(
        construct_data_model_validator(
            columns_info_key="internal_columns_info",
            expected_column_field_name_type_pairs=[
                (
                    "internal_record_creation_timestamp_column",
                    DBVarType.supported_timestamp_types(),
                ),
                ("internal_event_timestamp_column", DBVarType.supported_datetime_types()),
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
        Gets an EventView object from an EventTable object.

        Event views are typically used to create Lookup features for the event entity, to create Aggregate Over a
        Window features for other entities or enrich the item data by joining to the related Item view.

        You have the option to choose between two view construction modes: auto and manual, with auto being the
        default mode.

        When using the auto mode, the data accessed through the view is cleaned based on the default cleaning
        operations specified in the catalog table and special columns such as the record creation timestamp that
        are not intended for feature engineering are not included in the view columns.

        In manual mode, the default cleaning operations are not applied, and you have the flexibility to define your
        own cleaning operations.

        Parameters
        ----------
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL]
            View mode to use. When auto, the view will be constructed with cleaning operations from the table, the
            record creation timestamp column will be dropped.
        drop_column_names: Optional[List[str]]
            List of column names to drop (manual mode only).
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]]
            List of cleaning operations to apply per column in manual mode only. Each element in the list indicates
            the cleaning operations for a specific column. The association between this column and the cleaning
            operations is established via the ColumnCleaningOperation constructor.

        Returns
        -------
        EventView
            EventView object constructed from the source table.

        Examples
        --------
        Get an EventView in automated mode.

        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> event_view = event_table.get_view()


        Get an EventView in manual mode.

        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> event_view = event_table.get_view(
        ...     view_mode="manual",
        ...     drop_column_names=["record_available_at"],
        ...     column_cleaning_operations=[
        ...         fb.ColumnCleaningOperation(
        ...             column_name="Amount",
        ...             cleaning_operations=[
        ...                 fb.MissingValueImputation(imputed_value=0),
        ...                 fb.ValueBeyondEndpointImputation(
        ...                     type="less_than", end_point=0, imputed_value=None
        ...                 ),
        ...             ],
        ...         )
        ...     ],
        ... )
        """
        from featurebyte.api.event_view import EventView

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
            default_feature_job_setting=self.default_feature_job_setting,
            event_id_column=self.event_id_column,
            event_timestamp_schema=self.event_timestamp_schema,
        )

    @property
    def default_feature_job_setting(self) -> Optional[FeatureJobSettingUnion]:
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
    def event_timestamp_schema(self) -> Optional[TimestampSchema]:
        """
        Timestamp schema of the event timestamp column

        Returns
        -------
        Optional[TimestampSchema]
        """
        try:
            return self.cached_model.event_timestamp_schema
        except RecordRetrievalException:
            return self.internal_event_timestamp_schema

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
    def event_timestamp_timezone_offset(self) -> Optional[str]:
        """
        Timezone offset of the event timestamp column

        Returns
        -------
        Optional[str]
        """
        try:
            return self.cached_model.event_timestamp_timezone_offset
        except RecordRetrievalException:
            return self.internal_event_timestamp_timezone_offset

    @property
    def event_timestamp_timezone_offset_column(self) -> Optional[str]:
        """
        A column in the EventTable that contains the timezone offset of the event timestamp column

        Returns
        -------
        Optional[str]
        """
        try:
            return self.cached_model.event_timestamp_timezone_offset_column
        except RecordRetrievalException:
            return self.internal_event_timestamp_timezone_offset_column

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

    @classmethod
    def get_by_id(
        cls,
        id: ObjectId,
    ) -> EventTable:
        """
        Returns an EventTable object by its unique identifier (ID).

        Parameters
        ----------
        id: ObjectId
            EventTable unique identifier ID.

        Returns
        -------
        EventTable
            EventTable object.

        Examples
        --------
        Get an EventTable object that is already saved.

        >>> fb.EventTable.get_by_id(<event_table_id>)  # doctest: +SKIP
        """
        return cls._get_by_id(id=id)

    @typechecked
    def update_default_feature_job_setting(
        self, feature_job_setting: FeatureJobSettingUnion
    ) -> None:
        """
        Update default feature job setting

        Parameters
        ----------
        feature_job_setting: FeatureJobSettingUnion
            Feature job setting object. It can be an instance of FeatureJobSetting or
            CronFeatureJobSetting.

        Examples
        --------
        Configure a feature job setting to run daily at 1:05 am with a blind spot of 10 minutes.

        >>> from featurebyte import FeatureJobSetting
        >>> new_feature_job_setting = FeatureJobSetting(
        ...     blind_spot="10m",
        ...     period="24h",
        ...     offset="65m",
        ... )


        Update default feature job setting to the new feature job setting.

        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> event_table.update_default_feature_job_setting(
        ...     new_feature_job_setting
        ... )  # doctest: +SKIP

        See Also
        --------
        - [FeatureJobSetting](/reference/featurebyte.query_graph.model.feature_job_setting.FeatureJobSetting/):
            Class for specifying the scheduling of feature jobs.
        """
        self.update(
            update_payload={"default_feature_job_setting": feature_job_setting.model_dump()},
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
        Creates a new analysis of data availability and freshness of an event table in order to suggest an optimal
        setting for scheduling Feature Jobs and associated Blind Spot information.

        This analysis relies on the presence of record creation timestamps in the source table, typically added when
        updating data in the warehouse. The analysis focuses on a recent time window, the past four weeks by default.

        FeatureByte estimates the data update frequency based on the distribution of time intervals among the sequence
        of record creation timestamps. It also assesses the timeliness of source table updates and identifies late
        jobs using an outlier detection algorithm. By default, the recommended scheduling time takes late jobs into
        account.

        To accommodate data that may not arrive on time during warehouse updates, a blind spot is proposed for
        determining the cutoff for feature aggregation windows, in addition to scheduling frequency and time of
        the Feature Job. The suggested blind spot offers a percentage of late data closest to the user-defined
        tolerance, with a default of 0.005%.

        To validate the Feature Job schedule and blind spot recommendations, a backtest is conducted. You can also
        backtest your own settings.

        Parameters
        ----------
        analysis_date: Optional[datetime]
            Specifies the end date and time (in UTC) for the analysis. If not provided, the current date and time will
            be used.
        analysis_length: int
            Sets the duration of the analysis in seconds. The default value is 2,419,200 seconds (approximately
            4 weeks).
        min_featurejob_period: int
            Determines the minimum period (in seconds) between feature jobs. The default value is 60 seconds.
        exclude_late_job: bool
            If set to True, late jobs will be excluded from the analysis. This would assume that recent incidents
            won't happen again in the future.
        job_time_buffer_setting: Union[int, Literal["auto"]]
            Specifies the buffer time (in seconds) for job timing recommendations. A larger buffer reduces the risk
            of running a feature job before table updates are completed. If set to "auto", an appropriate buffer
            time will be determined automatically.
        blind_spot_buffer_setting: int
            Defines the buffer time (in seconds) for the blind spot recommendation. The default value is 5 seconds.
        late_data_allowance: float
            Indicates the maximum acceptable percentage of late records. The default value is 0.005% (5e-05).

        Returns
        -------
        FeatureJobSettingAnalysis

        Examples
        --------
        Create new feature job setting analysis on the saved event table with the following configuration:

        - analysis should cover the last 2 weeks,
        - recommendation for the feature job frequency period should be at least one hour,
        - recent late data warehouse updates should be excluded in the analysis, as it is expected they won't occur
          again because your instances have been upsized
        - tolerance for late data is incresed to 0.5%.

        >>> from datetime import datetime
        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> analysis = event_table.create_new_feature_job_setting_analysis(  # doctest: +SKIP
        ...     analysis_date=datetime.utcnow(),
        ...     analysis_length=60 * 60 * 24 * 7 * 12,
        ...     min_featurejob_period=60 * 60,
        ...     exclude_late_job=True,
        ...     blind_spot_buffer_setting=10,
        ...     job_time_buffer_setting=5,
        ...     late_data_allowance=0.5 / 100,
        ... )

        The analysis could also be called with default values.
        >>> default_analysis = (
        ...     event_table.create_new_feature_job_setting_analysis()
        ... )  # doctest: +SKIP

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
    def initialize_default_feature_job_setting(self, min_period_seconds: int = 3600) -> None:
        """
        Initializes default feature job setting by performing an analysis on the table to streamline the process of
        setting Feature Job. This analysis relies on the presence of record creation timestamps in the source table,
        typically added when updating data in the warehouse. The analysis focuses on a recent time window, the past
        four weeks by default.

        The Default Feature Job Setting establishes the default setting used by features that aggregate data in a
        table, ensuring consistency of the Feature Job Setting across features created by different team members.
        While it's possible to override the setting during feature declaration, using the Default Feature Job Setting
        simplifies the process of setting up the Feature Job Setting for each feature.

        The motivation for defining a Feature Job Setting early is to minimize inconsistencies between offline and
        online feature values.

        It is crucial to synchronize the frequency of batch feature computations with the frequency of source table
        refreshes, and to compute features after the source table refresh is fully completed. In addition, for
        historical serving to accurately replicate the production environment, it's essential to use data that was
        available at the historical points-in-time, considering the present or future latency of data. Latency of data
        refers to the time difference between the timestamp of an event and the timestamp at which the event data is
        accessible for ingestion. Any period during which data may be missing is referred to as a "blind spot".

        To address these challenges, the Feature Job setting in FeatureByte captures information about the frequency
        of batch feature computations, the timing of the batch process, and the assumed blind spot for the data. This
        helps ensure consistency between offline and online feature values, and accurate historical serving that
        reflects the conditions present in the production environment.

        Parameters
        ----------
        min_period_seconds: int
            Determines the minimum period (in seconds) between feature jobs. The default value is 3600 seconds (1 hour).

        Raises
        ------
        InvalidSettingsError
            Default feature job setting is already initialized

        Examples
        --------

        Initialize default feature job setting for the event table

        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> event_table.initialize_default_feature_job_setting()  # doctest: +SKIP

        """
        if self.default_feature_job_setting:
            raise InvalidSettingsError("Default feature job setting is already initialized")

        analysis = self.create_new_feature_job_setting_analysis(
            min_featurejob_period=min_period_seconds
        )
        self.update_default_feature_job_setting(analysis.get_recommendation())

    def list_feature_job_setting_analysis(self) -> Optional[pd.DataFrame]:
        """
        Lists feature job setting analyses that have been performed.

        Returns
        -------
        Optional[DataFrame]
            Table of feature job analysis

        Examples
        --------

        List feature job setting analysis

        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> analysis_list = event_table.list_feature_job_setting_analysis()

        """
        return FeatureJobSettingAnalysis.list(event_table_id=self.id)
