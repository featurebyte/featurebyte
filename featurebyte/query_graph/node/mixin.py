"""
This module contains mixins used in node classes
"""
# pylint: disable=too-few-public-methods
from typing import Any, Dict, List, Set

from abc import abstractmethod

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.metadata.operation import (
    AggregationColumn,
    DerivedDataColumn,
    NodeOutputCategory,
    OperationStructure,
    PostAggregationColumn,
    ViewDataColumn,
)


class SeriesOutputNodeOpStructMixin:
    """SeriesOutputNodeOpStructMixin class"""

    name: str
    transform_info: str
    output_type: NodeOutputType

    def _derive_node_operation_info(
        self, inputs: List[OperationStructure], visited_node_types: Set[NodeType]
    ) -> OperationStructure:
        """
        Derive node operation info

        Parameters
        ----------
        inputs: List[OperationStructure]
            List of input nodes' operation info
        visited_node_types: Set[NodeType]
            Set of visited nodes when doing backward traversal

        Returns
        -------
        OperationStructure
        """
        _ = visited_node_types
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
            **node_kwargs, output_type=self.output_type, output_category=output_category
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
        self, columns: List[ViewDataColumn], node_name: str
    ) -> List[AggregationColumn]:
        """
        Construct aggregations based on node parameters

        Parameters
        ----------
        columns: List[ViewDataColumn]
            Input columns
        node_name: str
            Node name

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
        self, inputs: List[OperationStructure], visited_node_types: Set[NodeType]
    ) -> OperationStructure:
        """
        Derive node operation info

        Parameters
        ----------
        inputs: List[OperationStructure]
            List of input nodes' operation info
        visited_node_types: Set[NodeType]
            Set of visited nodes when doing backward traversal

        Returns
        -------
        OperationStructure
        """
        input_operation_info = inputs[0]
        wanted_columns = set(self.get_required_input_columns())  # type: ignore
        columns = [col for col in input_operation_info.columns if col.name in wanted_columns]
        output_category = NodeOutputCategory.FEATURE
        if self.type == NodeType.ITEM_GROUPBY and NodeType.JOIN in visited_node_types:
            # if the output of the item_groupby will be used to join with other table,
            # this mean the output of this item_groupby is view but not feature.
            output_category = NodeOutputCategory.VIEW

        node_kwargs: Dict[str, Any] = {}
        if output_category == NodeOutputCategory.VIEW:
            node_kwargs["columns"] = [
                DerivedDataColumn.create(
                    name=name, columns=columns, transform=self.transform_info, node_name=self.name
                )
                for name in self.parameters.names
            ]
        else:
            node_kwargs["columns"] = columns
            node_kwargs["aggregations"] = self._get_aggregations(columns, node_name=self.name)

        return OperationStructure(
            **node_kwargs,
            output_type=self.output_type,
            output_category=output_category,
        )
