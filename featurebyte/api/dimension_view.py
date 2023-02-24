"""
DimensionView class
"""
from __future__ import annotations

from typing import Any, ClassVar, List, Literal, Optional, cast

from pydantic import Field
from typeguard import typechecked

from featurebyte.api.dimension_data import DimensionData
from featurebyte.api.scd_view import SlowlyChangingView
from featurebyte.api.view import View, ViewColumn
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import ViewMode
from featurebyte.exception import JoinViewMismatchError
from featurebyte.logger import logger
from featurebyte.query_graph.enum import GraphNodeType
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.table import DimensionTableData
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ColumnCleaningOperation, ViewMetadata


class DimensionViewColumn(ViewColumn):
    """
    DimensionViewColumn class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Column"])


class DimensionView(View):
    """
    Dimension Views allow users to transform Dimension Data.

    Transformations supported are the same as for EventView or ItemView except for lag that is not supported.
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["View"],
        proxy_class="featurebyte.DimensionView",
    )

    # class variables
    _series_class = DimensionViewColumn
    _view_graph_node_type: ClassVar[GraphNodeType] = GraphNodeType.DIMENSION_VIEW

    # pydantic instance variables
    dimension_id_column: str = Field(allow_mutation=False)

    @classmethod
    @typechecked
    def from_dimension_data(
        cls,
        dimension_data: DimensionData,
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL] = ViewMode.AUTO,
        drop_column_names: Optional[List[str]] = None,
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]] = None,
    ) -> DimensionView:
        """
        Construct an DimensionView object

        Parameters
        ----------
        dimension_data : DimensionData
            object used to construct DimensionView object
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL]
            View mode to use (manual or auto), when auto, the view will be constructed with cleaning operations
            from the data and the record creation date column will be dropped
        drop_column_names: Optional[List[str]]
            List of column names to drop (manual mode only)
        column_cleaning_operations: Optional[List[featurebyte.query_graph.node.nested.ColumnCleaningOperation]]
            Column cleaning operations to apply (manual mode only)

        Returns
        -------
        DimensionView
            constructed DimensionView object
        """
        cls._validate_view_mode_params(
            view_mode=view_mode,
            drop_column_names=drop_column_names,
            column_cleaning_operations=column_cleaning_operations,
        )

        # The input of view graph node is the data node. The final graph looks like this:
        #    +-----------+     +--------------------------------+
        #    | InputNode + --> | GraphNode(type:dimension_view) +
        #    +-----------+     +--------------------------------+
        drop_column_names = drop_column_names or []
        if view_mode == ViewMode.AUTO and dimension_data.record_creation_date_column:
            drop_column_names.append(dimension_data.record_creation_date_column)

        data_node = dimension_data.frame.node
        assert isinstance(data_node, InputNode)
        dimension_table_data = cast(DimensionTableData, dimension_data.table_data)
        column_cleaning_operations = column_cleaning_operations or []
        if view_mode == ViewMode.MANUAL:
            dimension_table_data = dimension_table_data.clone(
                column_cleaning_operations=column_cleaning_operations
            )

        view_graph_node, columns_info = dimension_table_data.construct_dimension_view_graph_node(
            dimension_data_node=data_node,
            drop_column_names=drop_column_names,
            metadata=ViewMetadata(
                view_mode=view_mode,
                drop_column_names=drop_column_names,
                column_cleaning_operations=column_cleaning_operations,
                data_id=data_node.parameters.id,
            ),
        )
        inserted_graph_node = GlobalQueryGraph().add_node(view_graph_node, input_nodes=[data_node])
        return DimensionView(
            feature_store=dimension_data.feature_store,
            tabular_source=dimension_data.tabular_source,
            columns_info=columns_info,
            node_name=inserted_graph_node.name,
            tabular_data_ids=[dimension_data.id],
            dimension_id_column=dimension_data.dimension_id_column,
        )

    @property
    def protected_attributes(self) -> list[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        list[str]
        """
        return super().protected_attributes + ["dimension_id_column"]

    @property
    def _getitem_frame_params(self) -> dict[str, Any]:
        """
        Parameters that will be passed to frame-like class constructor in __getitem__ method

        Returns
        -------
        dict[str, Any]
        """
        params = super()._getitem_frame_params
        params.update({"dimension_id_column": self.dimension_id_column})
        return params

    def validate_join(self, other_view: View) -> None:
        """
        Validate join should be implemented by view classes that have extra requirements.

        Parameters
        ---------
        other_view: View
            the other view that we are joining with

        Raises
        ------
        JoinViewMismatchError
            raised when the other view is a slowly changing view
        """
        if isinstance(other_view, SlowlyChangingView):
            logger.error("columns from a SlowlyChangingView can’t be added to a DimensionView")
            raise JoinViewMismatchError

    def get_join_column(self) -> str:
        return self.dimension_id_column
