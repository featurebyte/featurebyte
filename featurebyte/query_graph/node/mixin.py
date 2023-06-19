"""
This module contains mixins used in node classes
"""
from typing import Any, Dict, List, Optional, Set

from abc import abstractmethod

from pydantic import BaseModel

from featurebyte.enum import AggFunc, DBVarType
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.agg_func import construct_agg_func
from featurebyte.query_graph.node.metadata.column import InColumnStr
from featurebyte.query_graph.node.metadata.operation import (
    AggregationColumn,
    DerivedDataColumn,
    NodeOutputCategory,
    OperationStructure,
    OperationStructureBranchState,
    OperationStructureInfo,
    ViewDataColumn,
)


class BaseGroupbyParameters(BaseModel):
    """Common parameters related to groupby operation"""

    keys: List[InColumnStr]
    parent: Optional[InColumnStr]
    agg_func: AggFunc
    value_by: Optional[InColumnStr]
    serving_names: List[str]
    entity_ids: Optional[List[PydanticObjectId]]


class AggregationOpStructMixin:
    """AggregationOpStructMixin class"""

    name: str
    type: NodeType
    transform_info: str
    output_type: NodeOutputType
    parameters: Any

    @abstractmethod
    def _get_aggregations(
        self,
        columns: List[ViewDataColumn],
        node_name: str,
        other_node_names: Set[str],
        output_var_type: DBVarType,
    ) -> List[AggregationColumn]:
        """
        Construct aggregations based on node parameters

        Parameters
        ----------
        columns: List[ViewDataColumn]
            Input columns
        node_name: str
            Node name
        other_node_names: Set[str]
            Set of node names (input lineage)
        output_var_type: DBVarType
            Aggregation output variable type

        Returns
        -------
        List of aggregation columns
        """

    @abstractmethod
    def _exclude_source_columns(self) -> List[str]:
        """
        Exclude column to be include in source columns

        Returns
        -------
        List of excluded column names
        """

    @abstractmethod
    def _is_time_based(self) -> bool:
        """
        Returns whether the aggregation is time based

        Returns
        -------
        bool
        """

    def _get_parent_columns(self, columns: List[ViewDataColumn]) -> Optional[List[ViewDataColumn]]:
        """
        Get the table column used for aggregation

        Parameters
        ----------
        columns: List[ViewDataColumn]
            List of input columns

        Returns
        -------
        Optional[ViewDataColumn]
        """
        parent_columns = None
        if isinstance(self.parameters, BaseGroupbyParameters) and self.parameters.parent:
            parent_column = next(
                (col for col in columns if col.name == self.parameters.parent), None
            )
            if parent_column:
                parent_columns = [parent_column]
        return parent_columns

    def _get_agg_func(self) -> Optional[AggFunc]:
        """
        Retrieve agg_func from the parameters

        Returns
        -------
        Optional[str]
        """
        if isinstance(self.parameters, BaseGroupbyParameters):
            return self.parameters.agg_func
        return None

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        """
        Derive node operation info

        Parameters
        ----------
        inputs: List[OperationStructure]
            List of input nodes' operation info
        branch_state: OperationStructureBranchState
            State captures the graph branching state info
        global_state: OperationStructureInfo
            State captures the global graph info (used during operation structure derivation)

        Returns
        -------
        OperationStructure
        """
        _ = global_state
        input_operation_info = inputs[0]
        required_input_columns = self.get_required_input_columns(  # type: ignore
            input_index=0, available_column_names=input_operation_info.output_column_names
        )
        lineage_columns = set(required_input_columns)
        wanted_columns = lineage_columns
        if not global_state.keep_all_source_columns:
            wanted_columns = lineage_columns.difference(self._exclude_source_columns())
        parent_columns = self._get_parent_columns(input_operation_info.columns)
        agg_func = self._get_agg_func()
        if parent_columns:
            wanted_columns.update([parent_column.name for parent_column in parent_columns])

        other_node_names = set()
        columns = []
        for col in input_operation_info.columns:
            if col.name in lineage_columns:
                other_node_names.update(col.node_names)
            if col.name in wanted_columns:
                columns.append(col)

        columns = [col for col in input_operation_info.columns if col.name in wanted_columns]
        if not wanted_columns and agg_func == AggFunc.COUNT:
            # for groupby aggregation, the wanted columns is empty
            # in this case, take the first column from input operation info
            columns = input_operation_info.columns[:1]

        output_category = NodeOutputCategory.FEATURE
        if (
            self.type == NodeType.ITEM_GROUPBY
            and NodeType.JOIN_FEATURE in branch_state.visited_node_types
        ):
            # if the output of the item_groupby will be used to join with other table,
            # this mean the output of this item_groupby is view but not feature.
            output_category = NodeOutputCategory.VIEW
        elif self.type == NodeType.FORWARD_AGGREGATE:
            output_category = NodeOutputCategory.TARGET

        # prepare output variable type
        if agg_func:
            aggregation_func_obj = construct_agg_func(agg_func)
            input_var_type = parent_columns[0].dtype if parent_columns else columns[0].dtype
            output_var_type = aggregation_func_obj.derive_output_var_type(
                input_var_type=input_var_type, category=self.parameters.value_by
            )
        else:
            # to be overriden at the _get_aggregations
            output_var_type = DBVarType.UNKNOWN

        node_kwargs: Dict[str, Any] = {}
        if output_category == NodeOutputCategory.VIEW:
            node_kwargs["columns"] = [
                DerivedDataColumn.create(
                    name=self.parameters.name,
                    columns=columns,
                    transform=self.transform_info,
                    node_name=self.name,
                    other_node_names=other_node_names,
                    dtype=output_var_type,
                )
            ]
        else:
            node_kwargs["columns"] = columns
            node_kwargs["aggregations"] = self._get_aggregations(
                columns,
                node_name=self.name,
                other_node_names=other_node_names,
                output_var_type=output_var_type,
            )

        is_time_based = self._is_time_based()
        return OperationStructure(
            **node_kwargs,
            output_type=self.output_type,
            output_category=output_category,
            row_index_lineage=(self.name,),
            is_time_based=is_time_based,
        )
