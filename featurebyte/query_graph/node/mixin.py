"""
This module contains mixins used in node classes
"""
from typing import Any, Dict, List, Set

from abc import abstractmethod

from featurebyte.enum import AggFunc
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.metadata.operation import (
    AggregationColumn,
    DerivedDataColumn,
    NodeOutputCategory,
    OperationStructure,
    OperationStructureBranchState,
    OperationStructureInfo,
    PostAggregationColumn,
    ViewDataColumn,
)


class SeriesOutputNodeOpStructMixin:
    """SeriesOutputNodeOpStructMixin class"""

    name: str
    transform_info: str
    output_type: NodeOutputType

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
        _ = branch_state, global_state
        input_operation_info = inputs[0]
        output_category = input_operation_info.output_category
        columns = []
        aggregations = []
        for inp in inputs:
            columns.extend(inp.columns)
            aggregations.extend(inp.aggregations)

        node_kwargs: Dict[str, Any] = {}
        if output_category == NodeOutputCategory.VIEW:
            node_kwargs["columns"] = [
                DerivedDataColumn.create(
                    name=None, columns=columns, transform=self.transform_info, node_name=self.name
                )
            ]
        else:
            node_kwargs["columns"] = columns
            node_kwargs["aggregations"] = [
                PostAggregationColumn.create(
                    name=None,
                    columns=aggregations,
                    transform=self.transform_info,
                    node_name=self.name,
                )
            ]

        return OperationStructure(
            **node_kwargs,
            output_type=self.output_type,
            output_category=output_category,
            row_index_lineage=input_operation_info.row_index_lineage,
        )


class GroupbyNodeOpStructMixin:
    """GroupbyNodeOpStructMixin class"""

    name: str
    type: NodeType
    transform_info: str
    output_type: NodeOutputType
    parameters: Any

    @abstractmethod
    def _get_aggregations(
        self, columns: List[ViewDataColumn], node_name: str, other_node_names: Set[str]
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
        lineage_columns = set(self.get_required_input_columns())  # type: ignore
        wanted_columns = lineage_columns.difference(self._exclude_source_columns())
        other_node_names = set()
        columns = []
        for col in input_operation_info.columns:
            if col.name in lineage_columns:
                other_node_names.update(col.node_names)
            if col.name in wanted_columns:
                columns.append(col)

        columns = [col for col in input_operation_info.columns if col.name in wanted_columns]
        if not wanted_columns and getattr(self.parameters, "agg_func", None) == AggFunc.COUNT:
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

        node_kwargs: Dict[str, Any] = {}
        if output_category == NodeOutputCategory.VIEW:
            node_kwargs["columns"] = [
                DerivedDataColumn.create(
                    name=self.parameters.name,
                    columns=columns,
                    transform=self.transform_info,
                    node_name=self.name,
                    other_node_names=other_node_names,
                )
            ]
        else:
            node_kwargs["columns"] = columns
            node_kwargs["aggregations"] = self._get_aggregations(
                columns,
                node_name=self.name,
                other_node_names=other_node_names,
            )

        # Only item aggregates are not time based.
        is_time_based = self.type != NodeType.ITEM_GROUPBY
        return OperationStructure(
            **node_kwargs,
            output_type=self.output_type,
            output_category=output_category,
            row_index_lineage=(self.name,),
            is_time_based=is_time_based,
        )
