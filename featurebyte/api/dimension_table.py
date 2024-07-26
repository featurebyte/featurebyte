"""
DimensionTable class
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, List, Optional, Type, cast

from bson import ObjectId
from pydantic import Field, StrictStr, model_validator
from typing_extensions import Literal

from featurebyte.api.base_table import TableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.validator import construct_data_model_validator
from featurebyte.enum import DBVarType, TableDataType, ViewMode
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.dimension_table import DimensionTableModel
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.table import AllTableDataT, DimensionTableData
from featurebyte.query_graph.node.cleaning_operation import ColumnCleaningOperation
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ViewMetadata
from featurebyte.schema.dimension_table import DimensionTableCreate, DimensionTableUpdate

if TYPE_CHECKING:
    from featurebyte.api.dimension_view import DimensionView


class DimensionTable(TableApiObject):
    """
    A DimensionTable object represents a source table that holds static descriptive information.

    DimensionTable objects are created from a SourceTable object via the create_dimension_table method, and by
    identifying the column representing the primary key column of the source table (dimension_id_column).

    After creation, the table can optionally incorporate additional metadata at the column level to further aid
    feature engineering. This can include identifying columns that identify or reference entities, providing
    information about the semantics of the table columns, specifying default cleaning operations, or furnishing
    descriptions of its columns.

    Note that using a Dimension table requires special attention. If the data in the table changes slowly, it is
    not advisable to use it because these changes can cause significant data leaks during model training and
    adversely affect the inference performance. In such cases, it is recommended to use a Type 2 Slowly Changing
    Dimension table that maintains a history of changes."

    See Also
    --------
    - [create_dimension_table](/reference/featurebyte.api.source_table.SourceTable.create_dimension_table/): create dimension table from source table
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.DimensionTable",
        skip_params_and_signature_in_class_docs=True,
    )
    _route: ClassVar[str] = "/dimension_table"
    _update_schema_class: ClassVar[Any] = DimensionTableUpdate
    _create_schema_class: ClassVar[Any] = DimensionTableCreate
    _get_schema: ClassVar[Any] = DimensionTableModel
    _table_data_class: ClassVar[Type[AllTableDataT]] = DimensionTableData

    # pydantic instance variable (public)
    type: Literal[TableDataType.DIMENSION_TABLE] = TableDataType.DIMENSION_TABLE

    # pydantic instance variable (internal use)
    internal_dimension_id_column: StrictStr = Field(alias="dimension_id_column")

    # pydantic validators
    _model_validator = model_validator(mode="after")(
        construct_data_model_validator(
            columns_info_key="internal_columns_info",
            expected_column_field_name_type_pairs=[
                (
                    "internal_record_creation_timestamp_column",
                    DBVarType.supported_timestamp_types(),
                ),
                ("internal_dimension_id_column", DBVarType.supported_id_types()),
            ],
        )
    )

    def get_view(
        self,
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL] = ViewMode.AUTO,
        drop_column_names: Optional[List[str]] = None,
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]] = None,
    ) -> DimensionView:
        """
        Gets a DimensionView object from a DimensionTable object.

        Dimension views are commonly used to create Lookup features, and they can also be used to enrich views of
        other tables through joins.

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
            List of cleaning operations to apply per column in manual mode only. Each element in the list indicates the
            cleaning operations for a specific column. The association between this column and the cleaning operations
            is established via the ColumnCleaningOperation constructor.

        Returns
        -------
        DimensionView
            DimensionView object constructed from the source table.

        Examples
        --------
        Get a DimensionView in automated mode.

        >>> dimension_table = catalog.get_table("GROCERYPRODUCT")
        >>> dimension_view = dimension_table.get_view()


        Get a DimensionView in manual mode.

        >>> dimension_table = catalog.get_table("GROCERYPRODUCT")
        >>> dimension_view = dimension_table.get_view(
        ...     view_mode="manual",
        ...     drop_column_names=[],
        ...     column_cleaning_operations=[
        ...         fb.ColumnCleaningOperation(
        ...             column_name="ProductGroup",
        ...             cleaning_operations=[
        ...                 fb.MissingValueImputation(imputed_value="Unknown"),
        ...             ],
        ...         )
        ...     ],
        ... )
        """

        from featurebyte.api.dimension_view import DimensionView

        self._validate_view_mode_params(
            view_mode=view_mode,
            drop_column_names=drop_column_names,
            column_cleaning_operations=column_cleaning_operations,
        )

        # The input of view graph node is the table node. The final graph looks like this:
        #    +-----------+     +--------------------------------+
        #    | InputNode + --> | GraphNode(type:dimension_view) +
        #    +-----------+     +--------------------------------+
        drop_column_names = drop_column_names or []
        if view_mode == ViewMode.AUTO and self.record_creation_timestamp_column:
            drop_column_names.append(self.record_creation_timestamp_column)

        data_node = self.frame.node
        assert isinstance(data_node, InputNode)
        dimension_table_data = cast(DimensionTableData, self.table_data)
        column_cleaning_operations = column_cleaning_operations or []
        (
            dimension_table_data,
            column_cleaning_operations,
        ) = self._prepare_table_data_and_column_cleaning_operations(
            table_data=dimension_table_data,
            column_cleaning_operations=column_cleaning_operations,
            view_mode=view_mode,
        )

        view_graph_node, columns_info = dimension_table_data.construct_dimension_view_graph_node(
            dimension_table_node=data_node,
            drop_column_names=drop_column_names,
            metadata=ViewMetadata(
                view_mode=view_mode,
                drop_column_names=drop_column_names,
                column_cleaning_operations=column_cleaning_operations,
                table_id=data_node.parameters.id,
            ),
        )
        inserted_graph_node = GlobalQueryGraph().add_node(view_graph_node, input_nodes=[data_node])
        return DimensionView(
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            columns_info=columns_info,
            node_name=inserted_graph_node.name,
            dimension_id_column=self.dimension_id_column,
        )

    @property
    def dimension_id_column(self) -> str:
        """
        Returns the name of the column representing the primary key of the Dimension view.

        Returns
        -------
        str
        """
        try:
            return self.cached_model.dimension_id_column
        except RecordRetrievalException:
            return self.internal_dimension_id_column

    @property
    def timestamp_column(self) -> Optional[str]:
        return None

    @classmethod
    def get_by_id(
        cls,
        id: ObjectId,
    ) -> DimensionTable:
        """
        Returns a DimensionTable object by its unique identifier (ID).

        Parameters
        ----------
        id: ObjectId
            DimensionTable unique identifier ID.

        Returns
        -------
        DimensionTable
            DimensionTable object.

        Examples
        --------
        Get a DimensionTable object that is already saved.

        >>> fb.DimensionTable.get_by_id(<dimension_table_id>)  # doctest: +SKIP
        """
        return cls._get_by_id(id=id)
