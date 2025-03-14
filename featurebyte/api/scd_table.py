"""
SCDTable class
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, List, Optional, Tuple, Type, cast

from bson import ObjectId
from pydantic import Field, StrictStr, model_validator
from typeguard import typechecked
from typing_extensions import Literal

from featurebyte.api.base_table import TableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.validator import construct_data_model_validator
from featurebyte.enum import DBVarType, TableDataType, ViewMode
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.scd_table import SCDTableModel
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.model.table import AllTableDataT, SCDTableData
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.node.cleaning_operation import ColumnCleaningOperation
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ChangeViewMetadata, ViewMetadata
from featurebyte.schema.scd_table import SCDTableCreate, SCDTableUpdate

if TYPE_CHECKING:
    from featurebyte.api.change_view import ChangeView
    from featurebyte.api.scd_view import SCDView


class SCDTable(TableApiObject):
    """
    An SCDTable object represents a source table in the data warehouse that contains data that changes slowly and
    unpredictably over time. This table is commonly referred as a Slowly Changing Dimension (SCD) table.

    There are two main types of SCDs: Type 1, which overwrites old data with new data, and Type 2, which maintains a
    history of changes by creating a new record for each change. FeatureByte only supports the use of Type 2 SCDs since
    SCDs of Type 1 may cause data leaks during model training and poor performance during inference.

    An SCD table of Type 2 utilizes a natural key to distinguish each active row and facilitate tracking of changes
    over time. The SCD table employs effective and end (or expiration) timestamp columns to determine the active status
    of a row. In certain instances, an active flag column may replace the expiration timestamp column to indicate if a
    row is currently active.

    SCDTable objects are created from a SourceTable object via the create_scd_table method, and by identifying the
    columns representing the columns representing the natural key, the effective timestamp, and optionally the surrogate
    key, the end timestamp, and the active flag.

    After creation, the table can optionally incorporate additional metadata at the column level to further aid
    feature engineering. This can include identifying columns that identify or reference entities, providing
    information about the semantics of the table columns, specifying default cleaning operations, or furnishing
    descriptions of its columns.

    See Also
    --------
    - [create_scd_table](/reference/featurebyte.api.source_table.SourceTable.create_scd_table/): create SCD table from source table
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.SCDTable",
        skip_params_and_signature_in_class_docs=True,
    )
    _route: ClassVar[str] = "/scd_table"
    _update_schema_class: ClassVar[Any] = SCDTableUpdate
    _create_schema_class: ClassVar[Any] = SCDTableCreate
    _get_schema: ClassVar[Any] = SCDTableModel
    _table_data_class: ClassVar[Type[AllTableDataT]] = SCDTableData

    # pydantic instance variable (public)
    type: Literal[TableDataType.SCD_TABLE] = TableDataType.SCD_TABLE

    # pydantic instance variable (internal use)
    internal_default_feature_job_setting: Optional[FeatureJobSetting] = Field(
        alias="default_feature_job_setting", default=None
    )
    internal_natural_key_column: Optional[StrictStr] = Field(alias="natural_key_column")
    internal_effective_timestamp_column: StrictStr = Field(alias="effective_timestamp_column")
    internal_surrogate_key_column: Optional[StrictStr] = Field(
        alias="surrogate_key_column", default=None
    )
    internal_end_timestamp_column: Optional[StrictStr] = Field(
        alias="end_timestamp_column", default=None
    )
    internal_current_flag_column: Optional[StrictStr] = Field(
        alias="current_flag_column", default=None
    )
    internal_effective_timestamp_schema: Optional[TimestampSchema] = Field(
        alias="effective_timestamp_schema",
        default=None,
    )
    internal_end_timestamp_schema: Optional[TimestampSchema] = Field(
        alias="end_timestamp_schema",
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
                ("internal_natural_key_column", DBVarType.supported_id_types()),
                (
                    "internal_effective_timestamp_column",
                    DBVarType.supported_datetime_types(),
                ),
                ("internal_surrogate_key_column", DBVarType.supported_id_types()),
                ("internal_end_timestamp_column", DBVarType.supported_datetime_types()),
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
        Gets an SCDView object from a SCDTable object that represents a Slowly Changing Dimension (SCD) table.

        SCD views are typically used to create Lookup features for the entity represented by the natural key of the
        table or to create Aggregate As At features for other entities. They can also be used to enrich views of event
        or item tables through joins.

        You have the option to choose between two view construction modes: auto and manual, with auto being the
        default mode.

        When using the auto mode, the data accessed through the view is cleaned based on the default cleaning
        operations specified in the catalog table and special columns such as the record creation timestamp, the
        active flag and the experiation timestamp that are not intended for feature engineering are not included
        in the view columns.

        In manual mode, the default cleaning operations are not applied, and you have the flexibility to define
        your own cleaning operations.

        Parameters
        ----------
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL]
            View mode to use. When auto, the view will be constructed with cleaning operations from the table, the
            surrogate key, the end timestamp column, the active flag and the record creation timestamp column will be
            dropped.
        drop_column_names: Optional[List[str]]
            List of column names to drop (manual mode only).
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]]
            List of cleaning operations to apply per column in manual mode only. Each element in the list indicates the
            cleaning operations for a specific column. The association between this column and the cleaning operations
            is established via the ColumnCleaningOperation constructor.

        Returns
        -------
        SCDView
            SCDView object constructed from the source table.

        Examples
        --------
        Get a SCDView in automated mode.

        >>> scd_table = catalog.get_table("GROCERYCUSTOMER")
        >>> scd_view = scd_table.get_view()


        Get a SCDView in manual mode.

        >>> scd_table = catalog.get_table("GROCERYCUSTOMER")
        >>> scd_view = scd_table.get_view(
        ...     view_mode="manual",
        ...     drop_column_names=["record_available_at", "CurrentRecord"],
        ...     column_cleaning_operations=[
        ...         fb.ColumnCleaningOperation(
        ...             column_name="Gender",
        ...             cleaning_operations=[
        ...                 fb.MissingValueImputation(imputed_value="Unknown"),
        ...             ],
        ...         )
        ...     ],
        ... )
        """

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
        if view_mode == ViewMode.AUTO and self.current_flag_column:
            drop_column_names.append(self.current_flag_column)

        data_node = self.frame.node
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
            natural_key_column=self.natural_key_column,
            surrogate_key_column=self.surrogate_key_column,
            effective_timestamp_column=self.effective_timestamp_column,
            end_timestamp_column=self.end_timestamp_column,
            current_flag_column=self.current_flag_column,
            effective_timestamp_schema=self.effective_timestamp_schema,
            end_timestamp_schema=self.end_timestamp_schema,
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
        Gets a ChangeView from a Slowly Changing Dimension (SCD) table. The view offers a method to examine alterations
        that occur in a specific attribute within the natural key of the SCD table.

        To create the ChangeView, you need to provide the name of the SCD column for which you want to track changes
        through the track_changes_column parameter.

        Optionally,

        - you can define the default Feature Job Setting for the View. Default is once a day, at the time of the
        creation of the view.
        - you can provide a `prefix` parameter to control how the views columns are named.

        The resulting view has 5 columns:

        - the natural key of the SCDView
        - past_<name_of_column_tracked>: value of the column before the change
        - new_<name_of_column_tracked>: value of the column after the change
        - past_valid_from_timestamp (equal to the effective timestamp of the SCD before the change)
        - new_valid_from_timestamp (equal to the effective timestamp of the SCD after the change)

        The ChangeView can be used to create Aggregates of Changes Over a Window features, similar to Aggregates Over
        a Window features created from an Event View.

        Parameters
        ----------
        track_changes_column: str
            Name of the column to track changes for.
        default_feature_job_setting: Optional[FeatureJobSetting]
            Default feature job setting to set with the FeatureJobSetting constructor. If not provided, the default
            feature job setting is daily, aligning with the view's creation time.
        prefixes: Optional[Tuple[Optional[str], Optional[str]]]
            Optional prefixes where each element indicates the prefix to add to the new column names for the name of
            the column that we want to track. The first prefix will be used for the old, and the second for the new.
            If not provided, the column names will be prefixed with the default values of "past_", and "new_". At
            least one of the values must not be None. If two values are provided, they must be different.
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL]
            View mode to use. When auto, the view will be constructed with cleaning operations.
        drop_column_names: Optional[List[str]]
            List of column names to drop (manual mode only).
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]]
            List of cleaning operations to apply per column in manual mode only. Each element in the list indicates the
            cleaning operations for a specific column. The association between this column and the cleaning operations
            is established via the ColumnCleaningOperation constructor.

        Returns
        -------
        ChangeView
            ChangeView object constructed from the SCD source table.

        Examples
        --------
        Get a ChangeView to track changes in Customer's State.

        >>> scd_table = catalog.get_table("GROCERYCUSTOMER")
        >>> change_view = scd_table.get_change_view(
        ...     track_changes_column="State",
        ...     prefixes=("previous_", "next_"),
        ... )
        """

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
            default_feature_job_setting or self.default_feature_job_setting
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
            natural_key_column=self.natural_key_column,
            effective_timestamp_column=col_names.new_valid_from_column_name,
            default_feature_job_setting=feature_job_setting,
        )

    @property
    def default_feature_job_setting(self) -> Optional[FeatureJobSetting]:
        """
        Returns the default feature job setting for the SCDTable.

        Returns
        -------
        Optional[FeatureJobSetting]
        """
        try:
            return self.cached_model.default_feature_job_setting
        except RecordRetrievalException:
            return self.internal_default_feature_job_setting

    @property
    def natural_key_column(self) -> Optional[str]:
        """
        Returns the name of the column representing the natural key of a Slowly Changing Dimension (SCD) table. This
        column is used to distinguish each active row and facilitate tracking of changes over time.

        Returns
        -------
        Optional[str]
        """
        try:
            return self.cached_model.natural_key_column
        except RecordRetrievalException:
            return self.internal_natural_key_column

    @property
    def effective_timestamp_column(self) -> str:
        """
        Returns the name of the column representing the effective timestamp of a Slowly Changing Dimension (SCD) table.
        This column is used to indicate when a row started to be active.

        Returns
        -------
        str
        """
        try:
            return self.cached_model.effective_timestamp_column
        except RecordRetrievalException:
            return self.internal_effective_timestamp_column

    @property
    def effective_timestamp_schema(self) -> Optional[TimestampSchema]:
        """
        Returns the schema of the effective timestamp column

        Returns
        -------
        """
        try:
            return self.cached_model.effective_timestamp_schema
        except RecordRetrievalException:
            return self.internal_effective_timestamp_schema

    @property
    def end_timestamp_schema(self) -> Optional[TimestampSchema]:
        """
        Returns the schema of the end timestamp column

        Returns
        -------
        """
        try:
            return self.cached_model.end_timestamp_schema
        except RecordRetrievalException:
            return self.internal_end_timestamp_schema

    @property
    def surrogate_key_column(self) -> Optional[str]:
        """
        Returns the name of the column representing the surrogate key of a Slowly Changing Dimension (SCD) table.
        The column is an artificial key assigned by the system and is used as a unique identifier assigned to each
        record of the SCD table. As the column is not intended to be used for feature engineering, the views created
        from a SCD table do not contain this column by default.

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
        Returns the name of the column representing the end (or expiration) timestamp of a Slowly Changing Dimension
        (SCD) table. This column is used to indicate when a row stopped to be active. As the column is not intended to
        be used for feature engineering, the views created from a SCD table do not contain this column by default.

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
        Returns the name of the column representing the current flag of a Slowly Changing Dimension (SCD) table.
        This column is used to indicate if a row is currently active in the Data Warehouse. As the column is not
        intended to be used for feature engineering, the views created from a SCD table do not contain this
        column by default.

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
    def get_by_id(cls, id: ObjectId) -> SCDTable:
        """
        Returns a SCDTable object by its unique identifier (ID).

        Parameters
        ----------
        id: ObjectId
            SCDTable unique identifier ID.

        Returns
        -------
        SCDTable
            SCDTable object.

        Examples
        --------
        Get a SCDTable object that is already saved.

        >>> fb.SCDTable.get_by_id(<scd_table_id>)  # doctest: +SKIP
        """
        return cls._get_by_id(id=id)

    @typechecked
    def update_default_feature_job_setting(self, feature_job_setting: FeatureJobSetting) -> None:
        """
        Update default feature job setting

        Parameters
        ----------
        feature_job_setting: FeatureJobSetting
            Feature job setting object

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

        >>> scd_table = catalog.get_table("GROCERYCUSTOMER")
        >>> scd_table.update_default_feature_job_setting(new_feature_job_setting)  # doctest: +SKIP

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
