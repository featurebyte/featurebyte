"""
This module contains specialized table related models.
"""
from typing import Any, Dict, Iterable, List, Literal, Optional, Union, cast
from typing_extensions import Annotated  # pylint: disable=wrong-import-order

from abc import abstractmethod

from bson import ObjectId
from pydantic import Field, StrictStr

from featurebyte.enum import TableDataType
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.common_table import BaseTableData, TabularSource
from featurebyte.query_graph.model.critical_data_info import ImputeOperation
from featurebyte.query_graph.model.feature_store import FeatureStoreDetails
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.base import BaseNode
from featurebyte.query_graph.node.generic import InputNode, ProjectNode


class ConstructNodeMixin:
    """ConstructNodeMixin class"""

    type: Literal[
        TableDataType.GENERIC,
        TableDataType.EVENT_DATA,
        TableDataType.ITEM_DATA,
        TableDataType.DIMENSION_DATA,
        TableDataType.SCD_DATA,
    ]
    columns_info: List[ColumnInfo]
    tabular_source: TabularSource

    def _get_common_input_node_parameters(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "columns": [col.name for col in self.columns_info],
            "table_details": self.tabular_source.table_details,
        }

    def _iterate_column_info_with_imputations(self) -> Iterable[ColumnInfo]:
        """
        Iterate column info with imputation rules

        Yields
        ------
        ColumnInfo
            Column info with non-empty imputations
        """
        for col_info in self.columns_info:
            critical_data_info = col_info.critical_data_info
            if critical_data_info is not None and critical_data_info.imputations:
                yield col_info

    @staticmethod
    def _add_impute_operations(
        imputations: List[ImputeOperation],
        graph_node: GraphNode,
        frame_node: Node,
        project_node: ProjectNode,
        output_column_name: str,
    ) -> Node:
        """
        Add imputation operations to the graph node

        Parameters
        ----------
        imputations: List[ImputeOperation]
            List of imputation operations
        graph_node: GraphNode
            Graph node
        frame_node: Node
            Input frame node
        project_node: ProjectNode
            Column projection node
        output_column_name: str
            Output column name

        Returns
        -------
        Node
        """
        input_node: Node = project_node
        for imputation in imputations:
            condition_node = imputation.add_condition_operation(
                graph_node=graph_node, input_node=input_node
            )
            input_node = imputation.add_impute_operation(
                graph_node=graph_node, input_node=input_node, condition_node=condition_node
            )

        return graph_node.add_operation(
            node_type=NodeType.ASSIGN,
            node_params={"name": output_column_name, "value": None},
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[frame_node, input_node],
        )

    def construct_cleaning_recipe_node(self, input_node: InputNode) -> Optional[GraphNode]:
        """
        Construct cleaning recipe graph node

        Parameters
        ----------
        input_node: InputNode
            Input node for the cleaning recipe

        Returns
        -------
        Optional[GraphNode]
        """
        graph_node: Optional[GraphNode] = None
        frame_node: Optional[Node] = None
        proxy_input_nodes: List[BaseNode] = []
        for col_info in self._iterate_column_info_with_imputations():
            if graph_node is None:
                graph_node, proxy_input_nodes = GraphNode.create(
                    node_type=NodeType.PROJECT,
                    node_params={"columns": [col_info.name]},
                    node_output_type=NodeOutputType.SERIES,
                    input_nodes=[input_node],
                )

            proj_col_node = graph_node.add_operation(
                node_type=NodeType.PROJECT,
                node_params={"columns": [col_info.name]},
                node_output_type=NodeOutputType.SERIES,
                input_nodes=proxy_input_nodes,
            )

            assert col_info.critical_data_info is not None
            frame_node = self._add_impute_operations(
                imputations=col_info.critical_data_info.imputations,
                graph_node=graph_node,
                project_node=cast(ProjectNode, proj_col_node),
                frame_node=frame_node or proxy_input_nodes[0],
                output_column_name=col_info.name,
            )

        return graph_node

    @abstractmethod
    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        """
        Construct input node based on data info

        Parameters
        ----------
        feature_store_details: FeatureStoreDetails
            Feature store details

        Returns
        -------
        InputNode
        """


class GenericTableData(ConstructNodeMixin, BaseTableData):
    """GenericTableData class"""

    type: Literal[TableDataType.GENERIC] = Field(TableDataType.GENERIC, const=True)

    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        return InputNode(
            name="temp",
            parameters={
                "id": None,
                "feature_store_details": feature_store_details,
                **self._get_common_input_node_parameters(),
            },
        )


class EventTableData(ConstructNodeMixin, BaseTableData):
    """EventTableData class"""

    type: Literal[TableDataType.EVENT_DATA] = Field(TableDataType.EVENT_DATA, const=True)
    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")
    event_timestamp_column: StrictStr
    event_id_column: Optional[StrictStr] = Field(default=None)  # DEV-556: this should be compulsory

    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        return InputNode(
            name="temp",
            parameters={
                "id": self.id,
                "timestamp": self.event_timestamp_column,
                "feature_store_details": feature_store_details,
                **self._get_common_input_node_parameters(),
            },
        )


class ItemTableData(ConstructNodeMixin, BaseTableData):
    """ItemTableData class"""

    type: Literal[TableDataType.ITEM_DATA] = Field(TableDataType.ITEM_DATA, const=True)
    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")
    event_id_column: StrictStr
    item_id_column: StrictStr
    event_data_id: PydanticObjectId

    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        return InputNode(
            name="temp",
            parameters={
                "id": self.id,
                "feature_store_details": feature_store_details,
                **self._get_common_input_node_parameters(),
            },
        )


class DimensionTableData(ConstructNodeMixin, BaseTableData):
    """DimensionTableData class"""

    type: Literal[TableDataType.DIMENSION_DATA] = Field(TableDataType.DIMENSION_DATA, const=True)
    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")
    dimension_data_id_column: StrictStr

    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        return InputNode(
            name="temp",
            parameters={
                "id": self.id,
                "feature_store_details": feature_store_details,
                **self._get_common_input_node_parameters(),
            },
        )


class SCDTableData(ConstructNodeMixin, BaseTableData):
    """SCDTableData class"""

    type: Literal[TableDataType.SCD_DATA] = Field(TableDataType.SCD_DATA, const=True)
    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")
    natural_key_column: StrictStr
    effective_timestamp_column: StrictStr
    surrogate_key_column: Optional[StrictStr]
    end_timestamp_column: Optional[StrictStr] = Field(default=None)
    current_flag_column: Optional[StrictStr] = Field(default=None)

    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        return InputNode(
            name="temp",
            parameters={
                "id": self.id,
                "feature_store_details": feature_store_details,
                **self._get_common_input_node_parameters(),
            },
        )


SpecificTableData = Annotated[
    Union[EventTableData, ItemTableData, DimensionTableData, SCDTableData],
    Field(discriminator="type"),
]
