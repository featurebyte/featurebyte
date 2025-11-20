"""
SnapshotsTable class
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, List, Optional, Type, cast

from bson import ObjectId
from pydantic import Field, StrictStr, model_validator
from typeguard import typechecked
from typing_extensions import Literal

from featurebyte.api.base_table import TableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.validator import construct_data_model_validator
from featurebyte.enum import DBVarType, TableDataType, ViewMode
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.snapshots_table import SnapshotsTableModel
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.feature_job_setting import CronFeatureJobSetting
from featurebyte.query_graph.model.table import (
    AllTableDataT,
    SnapshotsTableData,
)
from featurebyte.query_graph.model.time_series_table import TimeInterval
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.node.cleaning_operation import ColumnCleaningOperation
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ViewMetadata
from featurebyte.schema.snapshots_table import SnapshotsTableCreate, SnapshotsTableUpdate

if TYPE_CHECKING:
    from featurebyte.api.snapshots_view import SnapshotsView


class SnapshotsTable(TableApiObject):
    """
    A SnapshotsTable object represents periodic snapshots of an entity’s state at regular time
    intervals.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.SnapshotsTable",
        skip_params_and_signature_in_class_docs=True,
    )
    _route: ClassVar[str] = "/snapshots_table"
    _update_schema_class: ClassVar[Any] = SnapshotsTableUpdate
    _create_schema_class: ClassVar[Any] = SnapshotsTableCreate
    _get_schema: ClassVar[Any] = SnapshotsTableModel
    _table_data_class: ClassVar[Type[AllTableDataT]] = SnapshotsTableData

    # pydantic instance variable (public)
    type: Literal[TableDataType.SNAPSHOTS_TABLE] = TableDataType.SNAPSHOTS_TABLE

    # pydantic instance variable (internal use)
    internal_default_feature_job_setting: Optional[CronFeatureJobSetting] = Field(
        alias="default_feature_job_setting", default=None
    )
    internal_snapshot_datetime_column: StrictStr = Field(alias="snapshot_datetime_column")
    internal_snapshot_datetime_schema: TimestampSchema = Field(alias="snapshot_datetime_schema")
    internal_time_interval: TimeInterval = Field(alias="time_interval")
    internal_series_id_column: StrictStr = Field(alias="series_id_column")

    # pydantic validators
    _model_validator = model_validator(mode="after")(
        construct_data_model_validator(
            columns_info_key="internal_columns_info",
            expected_column_field_name_type_pairs=[
                (
                    "internal_record_creation_timestamp_column",
                    DBVarType.supported_timestamp_types(),
                ),
                ("internal_snapshot_datetime_column", DBVarType.supported_ts_datetime_types()),
                ("internal_series_id_column", DBVarType.supported_id_types()),
            ],
        )
    )

    def get_view(
        self,
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL] = ViewMode.AUTO,
        drop_column_names: Optional[List[str]] = None,
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]] = None,
    ) -> SnapshotsView:
        """
        Gets an SnapshotsView object from an SnapshotsTable object.

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
        SnapshotsView
            SnapshotsView object constructed from the source table.

        Examples
        --------
        Get an SnapshotsView in automated mode.

        >>> snapshots_table = catalog.get_table("GROCERYPROFILES")  # doctest: +SKIP
        >>> snapshots_view = time_series_table.get_view()  # doctest: +SKIP


        Get an SnapshotsView in manual mode.

        >>> snapshots_table = catalog.get_table("GROCERYPROFILES")  # doctest: +SKIP
        >>> snapshots_view = time_series_table.get_view(
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
        ... )  # doctest: +SKIP
        """
        from featurebyte.api.snapshots_view import SnapshotsView

        self._validate_view_mode_params(
            view_mode=view_mode,
            drop_column_names=drop_column_names,
            column_cleaning_operations=column_cleaning_operations,
        )

        # The input of view graph node is the table node. The final graph looks like this:
        #    +-----------+     +----------------------------------+
        #    | InputNode + --> | GraphNode(type:snapshots_view) +
        #    +-----------+     +----------------------------------+
        drop_column_names = drop_column_names or []
        if view_mode == ViewMode.AUTO and self.record_creation_timestamp_column:
            drop_column_names.append(self.record_creation_timestamp_column)

        data_node = self.frame.node
        assert isinstance(data_node, InputNode)
        snapshots_table_data = cast(SnapshotsTableData, self.table_data)
        (
            snapshots_table_data,
            column_cleaning_operations,
        ) = self._prepare_table_data_and_column_cleaning_operations(
            table_data=snapshots_table_data,
            column_cleaning_operations=column_cleaning_operations,
            view_mode=view_mode,
        )

        view_graph_node, columns_info = snapshots_table_data.construct_snapshots_view_graph_node(
            snapshots_table_node=data_node,
            drop_column_names=drop_column_names,
            metadata=ViewMetadata(
                view_mode=view_mode,
                drop_column_names=drop_column_names,
                column_cleaning_operations=column_cleaning_operations,
                table_id=data_node.parameters.id,
            ),
        )
        inserted_graph_node = GlobalQueryGraph().add_node(view_graph_node, input_nodes=[data_node])
        columns_info = self._prepare_columns_info_for_view(
            view_node=inserted_graph_node, columns_info=columns_info
        )
        return SnapshotsView(
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            columns_info=columns_info,
            node_name=inserted_graph_node.name,
            default_feature_job_setting=self.default_feature_job_setting,
            series_id_column=self.series_id_column,
        )

    @property
    def default_feature_job_setting(self) -> Optional[CronFeatureJobSetting]:
        """
        Default feature job setting of the SnapshotsTable

        Returns
        -------
        Optional[CronFeatureJobSetting]
        """
        try:
            return self.cached_model.default_feature_job_setting
        except RecordRetrievalException:
            return self.internal_default_feature_job_setting

    @property
    def timestamp_column(self) -> Optional[str]:
        """
        Timestamp column name of the SnapshotsTable

        Returns
        -------
        Optional[str]
        """
        return self.snapshot_datetime_column

    @property
    def series_id_column(self) -> str:
        """
        Snapshot ID column name of the SnapshotsTable

        Returns
        -------
        str
        """
        try:
            return self.cached_model.series_id_column
        except RecordRetrievalException:
            return self.internal_series_id_column

    @property
    def snapshot_datetime_column(self) -> str:
        """
        Snapshot datetime column name of the SnapshotsTable

        Returns
        -------
        str
        """
        try:
            return self.cached_model.snapshot_datetime_column
        except RecordRetrievalException:
            return self.internal_snapshot_datetime_column

    @property
    def snapshot_datetime_schema(self) -> TimestampSchema:
        """
        Schema of the snapshot datetime column

        Returns
        -------
        TimestampSchema
        """
        try:
            return self.cached_model.snapshot_datetime_schema
        except RecordRetrievalException:
            return self.internal_snapshot_datetime_schema

    @property
    def time_interval(self) -> TimeInterval:
        """
        Time interval of the SnapshotsTable

        Returns
        -------
        TimeInterval
        """
        try:
            return self.cached_model.time_interval
        except RecordRetrievalException:
            return self.internal_time_interval

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
    ) -> SnapshotsTable:
        """
        Returns an SnapshotsTable object by its unique identifier (ID).

        Parameters
        ----------
        id: ObjectId
            SnapshotsTable unique identifier ID.

        Returns
        -------
        SnapshotsTable
            SnapshotsTable object.

        Examples
        --------
        Get an SnapshotsTable object that is already saved.

        >>> fb.SnapshotsTable.get_by_id(<snapshots_table_id>)  # doctest: +SKIP
        """
        return cls._get_by_id(id=id)

    @typechecked
    def update_default_feature_job_setting(
        self, feature_job_setting: CronFeatureJobSetting
    ) -> None:
        """
        Update default feature job setting

        Parameters
        ----------
        feature_job_setting: CronFeatureJobSetting
            Feature job setting object

        Examples
        --------
        Configure a feature job setting to run daily at 1:05 am..

        >>> from featurebyte import CronFeatureJobSetting, Crontab
        >>> new_feature_job_setting = CronFeatureJobSetting(
        ...     crontab="5 1 * * *",
        ...     timezone="Etc/UTC",
        ... )


        Update default feature job setting to the new feature job setting.

        >>> snapshots_table = catalog.get_table("GROCERYPROFILES")  # doctest: +SKIP
        >>> snapshots_table.update_default_feature_job_setting(
        ...     new_feature_job_setting
        ... )  # doctest: +SKIP

        See Also
        --------
        - [CronFeatureJobSetting](/reference/featurebyte.query_graph.model.feature_job_setting.CronFeatureJobSetting/):
            Class for specifying the cron job settings.
        """
        self.update(
            update_payload={"default_feature_job_setting": feature_job_setting.model_dump()},
            allow_update_local=True,
            add_internal_prefix=True,
        )
