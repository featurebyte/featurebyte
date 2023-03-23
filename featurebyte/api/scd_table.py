"""
SCDTable class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, List, Literal, Optional, Tuple, Type, cast

from pydantic import Field, StrictStr, root_validator

from featurebyte.api.base_table import TableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.validator import construct_data_model_root_validator
from featurebyte.enum import DBVarType, TableDataType, ViewMode
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.scd_table import SCDTableModel
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.model.table import AllTableDataT, SCDTableData
from featurebyte.query_graph.node.cleaning_operation import ColumnCleaningOperation
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ChangeViewMetadata, ViewMetadata
from featurebyte.schema.scd_table import SCDTableCreate, SCDTableUpdate

if TYPE_CHECKING:
    from featurebyte.api.change_view import ChangeView
    from featurebyte.api.scd_view import SCDView


class SCDTable(TableApiObject):
    """
    An SCD (slowly changing dimension) table is a type of FeatureByte table that represents a table in a data warehouse
    that contains data that changes slowly and unpredictably over time.

    There are two main types of SCDs: Type 1, which overwrites old data with new data, and Type 2, which maintains a
    history of changes by creating a new record for each change. FeatureByte only supports the use of Type 2 SCDs since
    SCDs of Type 1 may cause data leaks during model training and poor performance during inference.

    An SCD table of Type 2 utilizes a natural key to distinguish each active row and facilitate tracking of changes
    over time. The SCD table employs effective and expiration date columns to determine the active status of a row.
    In certain instances, an active flag column may replace the expiration date column to indicate if a row is
    currently active.

    To create an SCD table in FeatureByte, it is necessary to identify columns for the natural key, effective timestamp,
    and optionally surrogate key, expiration timestamp, and active flag.

    See Also
    --------
    - [create_scd_table](/reference/featurebyte.api.source_table.SourceTable.create_scd_table/): create SCD table from source table
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Table"], proxy_class="featurebyte.SCDTable")

    # class variables
    _route = "/scd_table"
    _update_schema_class = SCDTableUpdate
    _create_schema_class = SCDTableCreate
    _get_schema = SCDTableModel
    _table_data_class: ClassVar[Type[AllTableDataT]] = SCDTableData

    # pydantic instance variable (public)
    type: Literal[TableDataType.SCD_TABLE] = Field(TableDataType.SCD_TABLE, const=True)

    # pydantic instance variable (internal use)
    internal_natural_key_column: StrictStr = Field(alias="natural_key_column")
    internal_effective_timestamp_column: StrictStr = Field(alias="effective_timestamp_column")
    internal_surrogate_key_column: Optional[StrictStr] = Field(alias="surrogate_key_column")
    internal_end_timestamp_column: Optional[StrictStr] = Field(
        default=None, alias="end_timestamp_column"
    )
    internal_current_flag_column: Optional[StrictStr] = Field(
        default=None, alias="current_flag_column"
    )

    # pydantic validators
    _root_validator = root_validator(allow_reuse=True)(
        construct_data_model_root_validator(
            columns_info_key="internal_columns_info",
            expected_column_field_name_type_pairs=[
                (
                    "internal_record_creation_timestamp_column",
                    DBVarType.supported_timestamp_types(),
                ),
                ("internal_natural_key_column", DBVarType.supported_id_types()),
                ("internal_effective_timestamp_column", DBVarType.supported_timestamp_types()),
                ("internal_surrogate_key_column", DBVarType.supported_id_types()),
                ("internal_end_timestamp_column", DBVarType.supported_timestamp_types()),
                ("internal_current_flag_column", None),
            ],
        )
    )

    def get_view(
        self,
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL] = ViewMode.AUTO,
        drop_column_names: Optional[List[str]] = None,
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]] = None,
    ) -> SCDView:
        """
        Get a SCDView from the source table.

        You are able to specify the view construction mode to be auto or manual. In auto mode, the view will be
        constructed from the source table without any changes to the cleaning operations, or dropping column names.
        In manual mode, you are able to specify some overrides. However, the manual mode should not be commonly used
        as it might lead to unexpected behaviour if used wrongly.

        Parameters
        ----------
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL]
            View mode to use (manual or auto). When auto, the view will be constructed with cleaning operations
            from the table, the record creation timestamp column will be dropped and the columns to join from the
            EventView will be automatically selected.
        drop_column_names: Optional[List[str]]
            List of column names to drop (manual mode only).
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]]
            Column cleaning operations to apply (manual mode only).

        Returns
        -------
        SCDView
            SCDView object constructed from the source table.

        Examples
        --------
        Get a SCDView.

        >>> scd_table = fb.Table.get("GROCERYCUSTOMER")
        >>> scd_view = scd_table.get_view()
        """
        # pylint: disable=import-outside-toplevel
        from featurebyte.api.scd_view import SCDView

        self._validate_view_mode_params(
            view_mode=view_mode,
            drop_column_names=drop_column_names,
            column_cleaning_operations=column_cleaning_operations,
        )

        # The input of view graph node is the table node. The final graph looks like this:
        #    +-----------+     +--------------------------+
        #    | InputNode + --> | GraphNode(type:scd_view) +
        #    +-----------+     +--------------------------+
        drop_column_names = drop_column_names or []
        if view_mode == ViewMode.AUTO and self.record_creation_timestamp_column:
            drop_column_names.append(self.record_creation_timestamp_column)

        data_node = self.frame.node  # pylint: disable=duplicate-code
        assert isinstance(data_node, InputNode)
        scd_table_data = cast(SCDTableData, self.table_data)
        column_cleaning_operations = column_cleaning_operations or []
        (
            scd_table_data,
            column_cleaning_operations,
        ) = self._prepare_table_data_and_column_cleaning_operations(
            table_data=scd_table_data,
            column_cleaning_operations=column_cleaning_operations,
            view_mode=view_mode,
        )

        view_graph_node, columns_info = scd_table_data.construct_scd_view_graph_node(
            scd_table_node=data_node,
            drop_column_names=drop_column_names,
            metadata=ViewMetadata(
                view_mode=view_mode,
                drop_column_names=drop_column_names,
                column_cleaning_operations=column_cleaning_operations,
                table_id=data_node.parameters.id,
            ),
        )
        inserted_graph_node = GlobalQueryGraph().add_node(view_graph_node, input_nodes=[data_node])
        return SCDView(
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            columns_info=columns_info,
            node_name=inserted_graph_node.name,
            tabular_data_ids=[scd_table_data.id],
            natural_key_column=self.natural_key_column,
            surrogate_key_column=self.surrogate_key_column,
            effective_timestamp_column=self.effective_timestamp_column,
            end_timestamp_column=self.end_timestamp_column,
            current_flag_column=self.current_flag_column,
        )

    def get_change_view(
        self,
        track_changes_column: str,
        default_feature_job_setting: Optional[FeatureJobSetting] = None,
        prefixes: Optional[Tuple[Optional[str], Optional[str]]] = None,
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL] = ViewMode.AUTO,
        drop_column_names: Optional[List[str]] = None,
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]] = None,
    ) -> ChangeView:
        """
        Get a ChangeView from the SCD source table.

        You are able to specify the view construction mode to be auto or manual. In auto mode, the view will be
        constructed from the source table without any changes to the cleaning operations, or column names. In manual
        mode, you are able to specify some overrides. However, the manual mode should not be commonly used as it
        might lead to unexpected behaviour if used wrongly.

        Parameters
        ----------
        track_changes_column: str
            Column to track changes for.
        default_feature_job_setting: Optional[FeatureJobSetting]
            Default feature job setting to set.
        prefixes: Optional[Tuple[Optional[str], Optional[str]]]
            Optional prefixes where each element indicates the prefix to add to the new column names for the name of
            the column that we want to track. The first prefix will be used for the old, and the second for the new.
            Pass a value of None instead of a string to indicate that the column name will be prefixed with the default
            values of "past_", and "new_". At least one of the values must not be None. If two values are provided,
            they must be different.
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL]
            View mode to use (manual or auto). When auto, the view will be constructed with cleaning operations
            from the table, the record creation timestamp column will be dropped.
        drop_column_names: Optional[List[str]]
            List of column names to drop (manual mode only).
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]]
            Column cleaning operations to apply (manual mode only).

        Returns
        -------
        ChangeView
            ChangeView object constructed from the SCD source table.

        Examples
        --------
        Get a ChangeView.

        >>> scd_table = fb.Table.get("GROCERYCUSTOMER")
        >>> change_view = scd_table.get_change_view(
        ...     track_changes_column="CurrentRecord",
        ...     prefixes=("previous_", "next_"),
        ... )
        """
        # pylint: disable=import-outside-toplevel
        from featurebyte.api.change_view import ChangeView

        # Validate input
        ChangeView.validate_inputs(self, track_changes_column, prefixes)
        self._validate_view_mode_params(
            view_mode=view_mode,
            drop_column_names=drop_column_names,
            column_cleaning_operations=column_cleaning_operations,
        )

        # construct change view graph node from the scd table, the final graph looks like:
        #       +---------------------+    +-----------------------------+
        #       | InputNode(type:scd) | -->| GraphNode(type:change_view) |
        #       +---------------------+    +-----------------------------+
        feature_job_setting = ChangeView.get_default_feature_job_setting(
            default_feature_job_setting
        )
        col_names = SCDTableData.get_new_column_names(
            track_changes_column, self.effective_timestamp_column, prefixes
        )
        drop_column_names = drop_column_names or []
        if (
            view_mode == ViewMode.AUTO
            and self.record_creation_timestamp_column
            and self.record_creation_timestamp_column != track_changes_column
        ):
            drop_column_names.append(self.record_creation_timestamp_column)

        data_node = self.frame.node  # pylint disable=duplicate-code
        assert isinstance(data_node, InputNode)
        scd_table_data = cast(SCDTableData, self.table_data)
        column_cleaning_operations = column_cleaning_operations or []
        (
            scd_table_data,
            column_cleaning_operations,
        ) = self._prepare_table_data_and_column_cleaning_operations(
            table_data=scd_table_data,
            column_cleaning_operations=column_cleaning_operations,
            view_mode=view_mode,
        )

        view_graph_node, columns_info = scd_table_data.construct_change_view_graph_node(
            scd_table_node=data_node,
            track_changes_column=track_changes_column,
            prefixes=prefixes,
            drop_column_names=drop_column_names,
            metadata=ChangeViewMetadata(
                track_changes_column=track_changes_column,
                default_feature_job_setting=default_feature_job_setting,
                prefixes=prefixes,
                view_mode=view_mode,
                drop_column_names=drop_column_names,
                column_cleaning_operations=column_cleaning_operations,
                table_id=self.id,
            ),
        )
        inserted_graph_node = GlobalQueryGraph().add_node(view_graph_node, input_nodes=[data_node])
        return ChangeView(
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            columns_info=columns_info,
            node_name=inserted_graph_node.name,
            tabular_data_ids=[self.id],
            natural_key_column=self.natural_key_column,
            effective_timestamp_column=col_names.new_valid_from_column_name,
            default_feature_job_setting=feature_job_setting,
        )

    @property
    def natural_key_column(self) -> str:
        """
        Natural key column name of the SCDTable

        Returns
        -------
        str
        """
        try:
            return self.cached_model.natural_key_column
        except RecordRetrievalException:
            return self.internal_natural_key_column

    @property
    def effective_timestamp_column(self) -> str:
        """
        Effective timestamp column name of the SCDTable

        Returns
        -------
        str
        """
        try:
            return self.cached_model.effective_timestamp_column
        except RecordRetrievalException:
            return self.internal_effective_timestamp_column

    @property
    def surrogate_key_column(self) -> Optional[str]:
        """
        Surrogate key column name of the SCDTable

        Returns
        -------
        Optional[str]
        """
        try:
            return self.cached_model.surrogate_key_column
        except RecordRetrievalException:
            return self.internal_surrogate_key_column

    @property
    def end_timestamp_column(self) -> Optional[str]:
        """
        End timestamp column name of the SCDTable

        Returns
        -------
        Optional[str]
        """
        try:
            return self.cached_model.end_timestamp_column
        except RecordRetrievalException:
            return self.internal_end_timestamp_column

    @property
    def current_flag_column(self) -> Optional[str]:
        """
        Current flag column name of the SCDTable

        Returns
        -------
        Optional[str]
        """
        try:
            return self.cached_model.current_flag_column
        except RecordRetrievalException:
            return self.internal_current_flag_column

    @property
    def timestamp_column(self) -> Optional[str]:
        """
        Timestamp column name of the SCDTable

        Returns
        -------
        Optional[str]
        """
        return self.effective_timestamp_column
