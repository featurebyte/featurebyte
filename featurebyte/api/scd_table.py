"""
SCDTable class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, List, Literal, Optional, Tuple, Type, cast

from bson.objectid import ObjectId
from pydantic import Field, StrictStr, root_validator
from typeguard import typechecked

from featurebyte.api.base_table import TableApiObject
from featurebyte.api.source_table import SourceTable
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.validator import construct_data_model_root_validator
from featurebyte.enum import DBVarType, TableDataType, ViewMode
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.scd_data import SCDDataModel
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.model.table import AllTableDataT, SCDTableData
from featurebyte.query_graph.node.cleaning_operation import ColumnCleaningOperation
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ChangeViewMetadata, ViewMetadata
from featurebyte.schema.scd_data import SCDDataCreate, SCDDataUpdate

if TYPE_CHECKING:
    from featurebyte.api.change_view import ChangeView
    from featurebyte.api.scd_view import SlowlyChangingView


class SCDTable(TableApiObject):
    """
    SCDTable is a data source object connected with a Slowly Changing Dimension table of Type 2 in
    the data warehouse that has:\n
    - a natural key (key for which there is one unique active record)\n
    - a surrogate key (the primary key of the SCD)\n
    - an effective date or timestamp\n

    and optionally,\n
    - an end date or timestamp and\n
    - a current flag

    To create an instance of this class, see the `from_tabular_source` method.

    To build features, users can create SlowlyChangingViews from SCDTable.
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Table"], proxy_class="featurebyte.SCDTable")

    # class variables
    _route = "/scd_data"
    _update_schema_class = SCDDataUpdate
    _create_schema_class = SCDDataCreate
    _get_schema = SCDDataModel
    _table_data_class: ClassVar[Type[AllTableDataT]] = SCDTableData

    # pydantic instance variable (public)
    type: Literal[TableDataType.SCD_DATA] = Field(TableDataType.SCD_DATA, const=True)

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
                ("internal_record_creation_date_column", DBVarType.supported_timestamp_types()),
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
    ) -> SlowlyChangingView:
        """
        Construct an SlowlyChangingView object

        Parameters
        ----------
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL]
            View mode to use (manual or auto), when auto, the view will be constructed with cleaning operations
            from the data and the record creation date column will be dropped
        drop_column_names: Optional[List[str]]
            List of column names to drop (manual mode only)
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]]
            Column cleaning operations to apply (manual mode only)

        Returns
        -------
        SlowlyChangingView
            constructed SlowlyChangingView object
        """
        # pylint: disable=import-outside-toplevel
        from featurebyte.api.scd_view import SlowlyChangingView

        self._validate_view_mode_params(
            view_mode=view_mode,
            drop_column_names=drop_column_names,
            column_cleaning_operations=column_cleaning_operations,
        )

        # The input of view graph node is the data node. The final graph looks like this:
        #    +-----------+     +--------------------------+
        #    | InputNode + --> | GraphNode(type:scd_view) +
        #    +-----------+     +--------------------------+
        drop_column_names = drop_column_names or []
        if view_mode == ViewMode.AUTO and self.record_creation_date_column:
            drop_column_names.append(self.record_creation_date_column)

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
            scd_data_node=data_node,
            drop_column_names=drop_column_names,
            metadata=ViewMetadata(
                view_mode=view_mode,
                drop_column_names=drop_column_names,
                column_cleaning_operations=column_cleaning_operations,
                data_id=data_node.parameters.id,
            ),
        )
        inserted_graph_node = GlobalQueryGraph().add_node(view_graph_node, input_nodes=[data_node])
        return SlowlyChangingView(
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
        Create a change view from SCD data.

        Parameters
        ----------
        track_changes_column: str
            column to track changes for
        default_feature_job_setting: Optional[FeatureJobSetting]
            default feature job setting to set
        prefixes: Optional[Tuple[Optional[str], Optional[str]]]
            Optional prefixes where each element indicates the prefix to add to the new column names for the name of
            the column that we want to track. The first prefix will be used for the old, and the second for the new.
            Pass a value of None instead of a string to indicate that the column name will be prefixed with the default
            values of "past_", and "new_". At least one of the values must not be None. If two values are provided,
            they must be different.
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL]
            View mode to use (manual or auto), when auto, the view will be constructed with cleaning operations
            from the data and the record creation date column will be dropped
        drop_column_names: Optional[List[str]]
            List of column names to drop (manual mode only)
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]]
            Column cleaning operations to apply (manual mode only)

        Returns
        -------
        ChangeView
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

        # construct change view graph node from the scd data, the final graph looks like:
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
            and self.record_creation_date_column
            and self.record_creation_date_column != track_changes_column
        ):
            drop_column_names.append(self.record_creation_date_column)

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
            scd_data_node=data_node,
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
                data_id=self.id,
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

    @classmethod
    @typechecked
    def from_tabular_source(
        cls,
        tabular_source: SourceTable,
        name: str,
        natural_key_column: str,
        effective_timestamp_column: str,
        end_timestamp_column: Optional[str] = None,
        surrogate_key_column: Optional[str] = None,
        current_flag_column: Optional[str] = None,
        record_creation_date_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
    ) -> SCDTable:
        """
        Create SCDTable object from tabular source

        Parameters
        ----------
        tabular_source: SourceTable
            DatabaseTable object constructed from FeatureStore
        name: str
            SlowlyChanging data name
        natural_key_column: str
            Natural key column from the given tabular source
        effective_timestamp_column: str
            Effective timestamp column from the given tabular source
        end_timestamp_column: Optional[str]
            End timestamp column from the given tabular source
        surrogate_key_column: Optional[str]
            Surrogate key column from the given tabular source
        current_flag_column: Optional[str]
            Column to indicates whether the keys are for the current data point
        record_creation_date_column: str
            Record creation datetime column from the given tabular source
        _id: Optional[ObjectId]
            Identity value for constructed object

        Returns
        -------
        SCDTable

        Examples
        --------
        Create SCDTable from a table in the feature store

        >>> user_profiles = SCDTable.from_tabular_source(  # doctest: +SKIP
        ...    name="User Profiles",
        ...    tabular_source=feature_store.get_table(
        ...      database_name="DEMO",
        ...      schema_name="USER",
        ...      table_name="PROFILES"
        ...    ),
        ...    natural_key_column="USER_ID",
        ...    effective_timestamp_column="EFFECTIVE_AT",
        ...    end_timestamp_column="END_AT",
        ...    surrogate_key_column="ID",
        ...    current_flag_column="IS_CURRENT",
        ...    record_creation_date_column="RECORD_AVAILABLE_AT",
        ... )

        Get information about the SCDTable

        >>> user_profiles.info(verbose=True)  # doctest: +SKIP
        """
        return super().create(
            tabular_source=tabular_source,
            name=name,
            record_creation_date_column=record_creation_date_column,
            _id=_id,
            natural_key_column=natural_key_column,
            surrogate_key_column=surrogate_key_column,
            effective_timestamp_column=effective_timestamp_column,
            end_timestamp_column=end_timestamp_column,
            current_flag_column=current_flag_column,
        )
