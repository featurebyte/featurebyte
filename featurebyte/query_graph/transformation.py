"""
This module contains Query graph transformation related classes
"""
from typing import Any, Dict, Type, TypeVar, cast

from abc import abstractmethod

from featurebyte.enum import TableDataType
from featurebyte.query_graph.algorithm import dfs_traversal
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.base import BaseNode
from featurebyte.query_graph.node.generic import GroupbyNode as BaseGroupbyNode
from featurebyte.query_graph.node.generic import InputNode
from featurebyte.query_graph.node.generic import ItemGroupbyNode as BaseItemGroupbyNode
from featurebyte.query_graph.node.nested import BaseGraphNode, ProxyInputNode
from featurebyte.query_graph.transform.graph_pruning import GraphPruningExtractor
from featurebyte.query_graph.util import get_aggregation_identifier, get_tile_table_identifier


class BasePruningSensitiveNode(BaseNode):
    """
    Base class for nodes whose parameters have to be determined post pruning

    Nodes with this characteristic (e.g. those that derive some unique id based on its input node)
    should implement this interface.
    """

    @classmethod
    @abstractmethod
    def derive_parameters_post_prune(
        cls,
        graph: QueryGraphModel,
        input_node: Node,
        temp_node: "BasePruningSensitiveNode",
        pruned_graph: QueryGraphModel,
        pruned_input_node_name: str,
    ) -> Dict[str, Any]:
        """
        Derive additional parameters that should be based on pruned graph

        This method will be called by GraphReconstructor's add_pruning_sensitive_operation method.

        Parameters
        ----------
        graph: QueryGraphModel
            Query graph before pruning
        input_node: Node
            Input node of the current node of interest
        temp_node: BasePruningSensitiveNode
            A temporary instance of the current node of interest created for pruning purpose
        pruned_graph: QueryGraphModel
            Query graph after pruning
        pruned_input_node_name: str
            Name of the input node in the pruned graph after pruning
        """


class GroupbyNode(BaseGroupbyNode, BasePruningSensitiveNode):
    """An extended GroupbyNode that implements the derive_parameters_post_prune method"""

    @classmethod
    def derive_parameters_post_prune(
        cls,
        graph: QueryGraphModel,
        input_node: Node,
        temp_node: BasePruningSensitiveNode,
        pruned_graph: QueryGraphModel,
        pruned_input_node_name: str,
    ) -> Dict[str, Any]:
        table_details = None
        for node in dfs_traversal(graph, input_node):
            if isinstance(node, InputNode) and node.parameters.type == TableDataType.EVENT_DATA:
                # get the table details from the input node
                table_details = node.parameters.table_details.dict()
                break
        if table_details is None:
            raise ValueError("Failed to add groupby operation.")
        # tile_id & aggregation_id should be based on pruned graph to improve tile reuse
        tile_id = get_tile_table_identifier(
            table_details_dict=table_details, parameters=temp_node.parameters.dict()
        )
        aggregation_id = get_aggregation_identifier(
            transformations_hash=pruned_graph.node_name_to_ref[pruned_input_node_name],
            parameters=temp_node.parameters.dict(),
        )
        return {
            "tile_id": tile_id,
            "aggregation_id": aggregation_id,
        }


class ItemGroupbyNode(BaseItemGroupbyNode, BasePruningSensitiveNode):
    """An extended ItemGroupbyNode that implements the derive_parameters_post_prune method"""

    @classmethod
    def derive_parameters_post_prune(
        cls,
        graph: QueryGraphModel,
        input_node: Node,
        temp_node: BasePruningSensitiveNode,
        pruned_graph: QueryGraphModel,
        pruned_input_node_name: str,
    ) -> Dict[str, Any]:
        # TODO: derive aggregation id from input node hash and node parameters
        return {}


QueryGraphT = TypeVar("QueryGraphT", bound=QueryGraphModel)
PruningSensitiveNodeT = TypeVar("PruningSensitiveNodeT", bound=BasePruningSensitiveNode)


class GraphReconstructor:
    """GraphReconstructor class"""

    @classmethod
    def add_pruning_sensitive_operation(
        cls,
        graph: QueryGraphT,
        node_cls: Type[PruningSensitiveNodeT],
        node_params: Dict[str, Any],
        input_node: Node,
    ) -> PruningSensitiveNodeT:
        """
        Insert a pruning sensitive operation whose parameters can change after the graph is pruned

        One example is the groupby node whose tile_id and aggregation_id is a hash that includes the
        input node hash. To have stable tile_id and aggregation_id, always derive them after a
        pruning "dry run".

        Parameters
        ----------
        graph: QueryGraphT
            Query graph
        node_cls: Type[NodeT]
            Class of the node to be added
        node_params: Dict[str, Any]
            Node parameters
        input_node: Node
            Input node

        Returns
        -------
        PruningSensitiveNodeT
        """
        # create a temporary node & prune the graph before deriving additional parameters based on
        # the pruned graph
        temp_node = node_cls(name="temp", parameters=node_params)
        pruned_graph, node_name_map = GraphPruningExtractor(graph=graph).extract(
            node=input_node,
            target_columns=temp_node.get_required_input_columns(),
        )
        pruned_input_node_name = None
        for node in dfs_traversal(graph, input_node):
            if pruned_input_node_name is None and node.name in node_name_map:
                # as the input node could be pruned in the pruned graph, traverse the graph to find a valid input
                pruned_input_node_name = node_name_map[node.name]

        assert isinstance(pruned_input_node_name, str)
        additional_parameters = node_cls.derive_parameters_post_prune(
            graph=graph,
            input_node=input_node,
            temp_node=temp_node,
            pruned_graph=pruned_graph,
            pruned_input_node_name=pruned_input_node_name,
        )

        # insert the node by including the derived parameters (e.g. tile_id, aggregation_id, etc)
        node = graph.add_operation(
            node_type=temp_node.type,
            node_params={
                **temp_node.parameters.dict(),
                **additional_parameters,
            },
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[input_node],
        )
        return cast(PruningSensitiveNodeT, node)

    @classmethod
    def reconstruct(
        cls,
        graph: QueryGraphT,
        output_graph: QueryGraphT,
        replace_nodes_map: Dict[str, Node],
        regenerate_groupby_hash: bool = False,
    ) -> QueryGraphT:
        """
        Reconstruct the query graph using the replacement node mapping

        Parameters
        ----------
        graph: QueryGraphT
            Query graph
        output_graph: QueryGraphT
            Output query graph
        replace_nodes_map: Dict[str, Node]
            Node name (of the input query graph) to replacement node mapping
        regenerate_groupby_hash: bool
            Whether to regenerate tile ID & aggregation ID in groupby node

        Returns
        -------
        QueryGraphT
        """
        for _node in graph.iterate_sorted_nodes():
            input_node_names = graph.backward_edges_map[_node.name]
            node = replace_nodes_map.get(_node.name, graph.nodes_map[_node.name])
            input_nodes = [
                replace_nodes_map.get(input_node_name, graph.nodes_map[input_node_name])
                for input_node_name in input_node_names
            ]
            if node.type == NodeType.GROUPBY and regenerate_groupby_hash:
                cls.add_pruning_sensitive_operation(
                    graph=output_graph,
                    node_cls=GroupbyNode,
                    node_params=node.parameters.dict(),
                    input_node=input_nodes[0],
                )
            else:
                output_graph.add_operation(
                    node_type=node.type,
                    node_params=node.parameters.dict(),
                    node_output_type=node.output_type,
                    input_nodes=input_nodes,
                )
        return output_graph


class GraphFlattener:
    """GraphFlattener class"""

    @classmethod
    def flatten(cls, graph: QueryGraphT, output_graph: QueryGraphT) -> QueryGraphT:
        """
        Flatten the query graph by removing graph node

        Parameters
        ----------
        graph: QueryGraphT
            QueryGraph to be flattened
        output_graph: QueryGraphT
            Output QueryGraph

        Returns
        -------
        QueryGraphT
        """
        # node_name_map: key(this-graph-node-name) => value(flattened-graph-node-name)
        node_name_map: Dict[str, str] = {}
        for node in graph.iterate_sorted_nodes():
            if isinstance(node, BaseGraphNode):
                nested_graph = node.parameters.graph.flatten()
                # nested_node_name_map: key(nested-node-name) => value(flattened-graph-node-name)
                nested_node_name_map: Dict[str, str] = {}
                for nested_node in nested_graph.iterate_sorted_nodes():
                    input_nodes = []
                    for input_node_name in nested_graph.backward_edges_map[nested_node.name]:
                        input_nodes.append(
                            output_graph.get_node_by_name(nested_node_name_map[input_node_name])
                        )

                    if isinstance(nested_node, ProxyInputNode):
                        nested_node_name_map[nested_node.name] = node_name_map[
                            nested_node.parameters.node_name
                        ]
                    else:
                        inserted_node = output_graph.add_operation(
                            node_type=nested_node.type,
                            node_params=nested_node.parameters.dict(),
                            node_output_type=nested_node.output_type,
                            input_nodes=input_nodes,
                        )
                        nested_node_name_map[nested_node.name] = inserted_node.name

                    if nested_node.name == node.parameters.output_node_name:
                        node_name_map[node.name] = nested_node_name_map[nested_node.name]
            else:
                inserted_node = output_graph.add_operation(
                    node_type=node.type,
                    node_params=node.parameters.dict(),
                    node_output_type=node.output_type,
                    input_nodes=[
                        output_graph.get_node_by_name(node_name_map[input_node_name])
                        for input_node_name in graph.backward_edges_map[node.name]
                    ],
                )
                node_name_map[node.name] = inserted_node.name
        return output_graph
