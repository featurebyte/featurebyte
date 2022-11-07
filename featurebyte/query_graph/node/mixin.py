"""
This module contains mixins used in node classes
"""
# pylint: disable=too-few-public-methods
from typing import Any, Dict, List, Optional

from abc import abstractmethod

from featurebyte.query_graph.enum import NodeOutputType
from featurebyte.query_graph.node.metadata.operation import (
    AggregationColumn,
    DerivedDataColumn,
    NodeOutputCategory,
    NodeTransform,
    OperationStructure,
    PostAggregationColumn,
    ViewDataColumn,
)


class SeriesOutputNodeOpStructMixin:
    """SeriesOutputNodeOpStructMixin class"""

    transform_info: NodeTransform
    output_type: NodeOutputType

    def _get_output_name(self) -> Optional[str]:
        """
        Get the node output name

        Returns
        -------
        Optional[str]
            Node name
        """
        return None

    def derive_node_operation_info(self, inputs: List[OperationStructure]) -> OperationStructure:
        """
        Derive node operation info

        Parameters
        ----------
        inputs: List[OperationStructure]
            List of input nodes' operation info

        Returns
        -------
        OperationStructure
        """
        input_operation_info = inputs[0]
        output_category = input_operation_info.output_category
        columns = []
        aggregations = []
        for inp in inputs:
            columns.extend(inp.columns)
            aggregations.extend(inp.aggregations)

        node_kwargs: Dict[str, Any] = {}
        output_name: Optional[str] = self._get_output_name()
        if output_category == NodeOutputCategory.VIEW:
            node_kwargs["columns"] = [
                DerivedDataColumn.create(
                    name=output_name, columns=columns, transform=self.transform_info
                )
            ]
        else:
            node_kwargs["columns"] = columns
            node_kwargs["aggregations"] = [
                PostAggregationColumn.create(
                    name=output_name, columns=aggregations, transform=self.transform_info
                )
            ]

        return OperationStructure(
            **node_kwargs, output_type=self.output_type, output_category=output_category
        )


class GroupbyNodeOpStructMixin:
    """GroupbyNodeOpStructMixin class"""

    transform_info: NodeTransform
    output_type: NodeOutputType
    parameters: Any

    @abstractmethod
    def _get_aggregations(self, columns: List[ViewDataColumn]) -> List[AggregationColumn]:
        """
        Construct aggregations based on node parameters

        Parameters
        ----------
        columns: List[ViewDataColumn]
            Input columns

        Returns
        -------
        List of aggregation columns
        """

    def derive_node_operation_info(self, inputs: List[OperationStructure]) -> OperationStructure:
        """
        Derive node operation info

        Parameters
        ----------
        inputs: List[OperationStructure]
            List of input nodes' operation info

        Returns
        -------
        OperationStructure
        """
        input_operation_info = inputs[0]
        columns = [
            col
            for col in input_operation_info.columns
            if col.name in self.get_required_input_columns()  # type: ignore
        ]
        return OperationStructure(
            columns=columns,
            aggregations=self._get_aggregations(columns),
            output_type=self.output_type,
            output_category=NodeOutputCategory.FEATURE,
        )
