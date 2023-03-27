"""
DimensionTable class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, List, Literal, Optional, Type, cast

from pydantic import Field, StrictStr, root_validator

from featurebyte.api.base_table import TableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.validator import construct_data_model_root_validator
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
    A Dimension table is a FeatureByte table that represents a table in the data warehouse that stores static
    descriptive information such as a birth date.

    Using a Dimension table requires special attention. If the data in the table changes slowly, it is not advisable to
    use it because these changes can cause significant data leaks during model training and adversely affect the
    inference performance. In such cases, it is recommended to use a Slowly Changing Dimension (SCD) table of Type
    2 that maintains a history of changes.

    To create a Dimension Table in FeatureByte, it is necessary to identify which column represents the primary key.

    See Also
    --------
    - [create_dimension_table](/reference/featurebyte.api.source_table.SourceTable.create_dimension_table/): create dimension table from source table
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Table"], proxy_class="featurebyte.DimensionTable")

    # class variables
    _route = "/dimension_table"
    _update_schema_class = DimensionTableUpdate
    _create_schema_class = DimensionTableCreate
    _get_schema = DimensionTableModel
    _table_data_class: ClassVar[Type[AllTableDataT]] = DimensionTableData

    # pydantic instance variable (public)
    type: Literal[TableDataType.DIMENSION_TABLE] = Field(TableDataType.DIMENSION_TABLE, const=True)

    # pydantic instance variable (internal use)
    internal_dimension_id_column: StrictStr = Field(alias="dimension_id_column")

    # pydantic validators
    _root_validator = root_validator(allow_reuse=True)(
        construct_data_model_root_validator(
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
        Get a DimensionView from a dimension table.

        You are able to specify the view construction mode to be auto or manual. In auto mode, the view will be
        constructed from the source table without any changes to the cleaning operations, or dropping column names.
        In manual mode, you are able to specify some overrides. However, the manual mode should not be commonly used
        as it might lead to unexpected behaviour if used wrongly.

        Parameters
        ----------
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL]
            View mode to use (manual or auto). When auto, the view will be constructed with cleaning operations
            from the table, the record creation timestamp column will be dropped.
        drop_column_names: Optional[List[str]]
            List of column names to drop (manual mode only).
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]]
            Column cleaning operations to apply (manual mode only).

        Returns
        -------
        DimensionView
            DimensionView object constructed from the source table.

        Examples
        --------
        Get a DimensionView.

        >>> dimension_table = fb.Table.get("GROCERYPRODUCT")
        >>> dimension_view = dimension_table.get_view()
        """
        # pylint: disable=import-outside-toplevel
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
            tabular_data_ids=[self.id],
            dimension_id_column=self.dimension_id_column,
        )

    @property
    def dimension_id_column(self) -> str:
        """
        Dimension ID column name of the DimensionTable

        Returns
        -------
        str
        """
        try:
            return self.cached_model.dimension_id_column
        except RecordRetrievalException:
            return self.internal_dimension_id_column
