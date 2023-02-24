"""
SlowlyChangingView class
"""
from __future__ import annotations

from typing import Any, ClassVar, List, Optional, cast

from pydantic import Field
from typeguard import typechecked

from featurebyte.api.scd_data import SlowlyChangingData
from featurebyte.api.view import GroupByMixin, View, ViewColumn
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import ViewMode
from featurebyte.exception import JoinViewMismatchError
from featurebyte.logger import logger
from featurebyte.query_graph.enum import GraphNodeType
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.table import SCDTableData
from featurebyte.query_graph.node.generic import SCDBaseParameters
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ColumnCleaningOperation, ViewMetadata


class SlowlyChangingViewColumn(ViewColumn):
    """
    SlowlyChangingViewColumn class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Column"])


class SlowlyChangingView(View, GroupByMixin):
    """
    SlowlyChangingView's allow users to transform Slowly Changing Dimension Data.

    Transformations supported are the same as for EventView or ItemView except for lag that is not supported.
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["View"],
        proxy_class="featurebyte.SlowlyChangingView",
    )

    # class variables
    _series_class = SlowlyChangingViewColumn
    _view_graph_node_type: ClassVar[GraphNodeType] = GraphNodeType.SCD_VIEW

    # pydantic instance variables
    natural_key_column: str = Field(allow_mutation=False)
    effective_timestamp_column: str = Field(allow_mutation=False)
    surrogate_key_column: Optional[str] = Field(allow_mutation=False)
    end_timestamp_column: Optional[str] = Field(allow_mutation=False)
    current_flag_column: Optional[str] = Field(allow_mutation=False)

    @property
    def timestamp_column(self) -> Optional[str]:
        return self.effective_timestamp_column

    @classmethod
    @typechecked
    def from_slowly_changing_data(
        cls,
        slowly_changing_data: SlowlyChangingData,
        view_mode: ViewMode = ViewMode.AUTO,
        drop_column_names: Optional[List[str]] = None,
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]] = None,
    ) -> SlowlyChangingView:
        """
        Construct an SlowlyChangingView object

        Parameters
        ----------
        slowly_changing_data : SlowlyChangingData
            object used to construct SlowlyChangingView object
        view_mode: ViewMode
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
        cls._validate_view_mode_params(
            view_mode=view_mode,
            drop_column_names=drop_column_names,
            column_cleaning_operations=column_cleaning_operations,
        )

        # The input of view graph node is the data node. The final graph looks like this:
        #    +-----------+     +--------------------------+
        #    | InputNode + --> | GraphNode(type:scd_view) +
        #    +-----------+     +--------------------------+
        drop_column_names = drop_column_names or []
        if view_mode == ViewMode.AUTO and slowly_changing_data.record_creation_date_column:
            drop_column_names.append(slowly_changing_data.record_creation_date_column)

        data_node = slowly_changing_data.frame.node
        assert isinstance(data_node, InputNode)
        scd_table_data = cast(SCDTableData, slowly_changing_data.table_data)
        column_cleaning_operations = column_cleaning_operations or []
        if column_cleaning_operations:
            scd_table_data = scd_table_data.clone(
                column_cleaning_operations=column_cleaning_operations
            )

        view_graph_node, columns_info = scd_table_data.construct_scd_view_graph_node(
            scd_data_node=data_node,
            drop_column_names=drop_column_names,
            view_mode=view_mode,
            metadata=ViewMetadata(
                view_mode=view_mode,
                drop_column_names=drop_column_names,
                column_cleaning_operations=column_cleaning_operations,
                data_id=data_node.parameters.id,
            ),
        )
        inserted_graph_node = GlobalQueryGraph().add_node(view_graph_node, input_nodes=[data_node])
        return SlowlyChangingView(
            feature_store=slowly_changing_data.feature_store,
            tabular_source=slowly_changing_data.tabular_source,
            columns_info=columns_info,
            node_name=inserted_graph_node.name,
            tabular_data_ids=[scd_table_data.id],
            natural_key_column=slowly_changing_data.natural_key_column,
            surrogate_key_column=slowly_changing_data.surrogate_key_column,
            effective_timestamp_column=slowly_changing_data.effective_timestamp_column,
            end_timestamp_column=slowly_changing_data.end_timestamp_column,
            current_flag_column=slowly_changing_data.current_flag_column,
        )

    @property
    def protected_attributes(self) -> list[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        list[str]
        """
        return super().protected_attributes + [
            "natural_key_column",
            "surrogate_key_column",
            "effective_timestamp_column",
            "end_timestamp_column",
            "current_flag_column",
        ]

    def _get_additional_inherited_columns(self) -> set[str]:
        columns = {self.effective_timestamp_column}
        if self.current_flag_column is not None:
            columns.add(self.current_flag_column)
        return columns

    @property
    def _getitem_frame_params(self) -> dict[str, Any]:
        """
        Parameters that will be passed to frame-like class constructor in __getitem__ method

        Returns
        -------
        dict[str, Any]
        """
        params = super()._getitem_frame_params
        params.update(
            {
                "natural_key_column": self.natural_key_column,
                "surrogate_key_column": self.surrogate_key_column,
                "effective_timestamp_column": self.effective_timestamp_column,
                "end_timestamp_column": self.end_timestamp_column,
                "current_flag_column": self.current_flag_column,
            }
        )
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
            logger.error("columns from a SlowlyChangingView can’t be added to a SlowlyChangingView")
            raise JoinViewMismatchError

    def get_join_column(self) -> str:
        return self.natural_key_column

    def get_common_scd_parameters(self) -> SCDBaseParameters:
        """
        Get parameters related to Slowly Changing Data (SCD)

        Returns
        -------
        SCDBaseParameters
        """
        return SCDBaseParameters(
            effective_timestamp_column=self.effective_timestamp_column,
            natural_key_column=self.natural_key_column,
            current_flag_column=self.current_flag_column,
            end_timestamp_column=self.end_timestamp_column,
        )

    def _get_join_parameters(self, calling_view: View) -> dict[str, Any]:

        # When calling_view doesn't have the timestamp_column attribute, it means that it is a
        # DimensionView. It is invalid to join DimensionView with SlowlyChangingView on the right
        # side. A validation error would have been raised before reaching here.
        assert hasattr(calling_view, "timestamp_column") and isinstance(
            calling_view.timestamp_column, str
        )

        left_timestamp_column = calling_view.timestamp_column
        return {
            "scd_parameters": {
                "left_timestamp_column": left_timestamp_column,
                **self.get_common_scd_parameters().dict(),
            }
        }

    def _get_additional_excluded_columns_as_other_view(self) -> list[str]:
        excluded_columns = [self.effective_timestamp_column]
        if self.current_flag_column:
            excluded_columns.append(self.current_flag_column)
        if self.surrogate_key_column:
            excluded_columns.append(self.surrogate_key_column)
        if self.end_timestamp_column:
            excluded_columns.append(self.end_timestamp_column)
        return excluded_columns

    def _get_as_feature_parameters(self, offset: Optional[str] = None) -> dict[str, Any]:
        return {
            "scd_parameters": {
                "offset": offset,
                **self.get_common_scd_parameters().dict(),
            }
        }
