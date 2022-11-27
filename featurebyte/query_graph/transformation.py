"""
This module contains Query graph transformation related classes
"""
# pylint: disable=too-few-public-methods
from typing import Any, Dict, Set, Tuple, Type, TypeVar, cast

from featurebyte.query_graph.algorithm import dfs_traversal
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import (
    AssignNode,
    GroupbyNode,
    ParametersDerivedPostPruneNode,
)
from featurebyte.query_graph.node.nested import BaseGraphNode, ProxyInputNode

QueryGraphT = TypeVar("QueryGraphT", bound=QueryGraphModel)
NodeT = TypeVar("NodeT", bound=ParametersDerivedPostPruneNode)


class GraphPruner:
    """GraphPruner class"""

    @classmethod
    def _prune(
        cls,
        graph: QueryGraphT,
        target_node: Node,
        target_columns: Set[str],
        pruned_graph: QueryGraphT,
        processed_node_names: Set[str],
        node_name_map: Dict[str, str],
        topological_order_map: Dict[str, int],
    ) -> QueryGraphT:
        # pruning: move backward from target node to the input node
        to_prune_target_node = False
        input_node_names = graph.backward_edges_map.get(target_node.name, [])

        if isinstance(target_node, AssignNode):
            # check whether to keep the current assign node
            assign_column_name = target_node.parameters.name
            if assign_column_name in target_columns:
                # remove matched name from the target_columns
                target_columns -= {assign_column_name}
            else:
                # remove series path if exists
                to_prune_target_node = True
                input_node_names = input_node_names[:1]

        # If the current target node produces a new column, we should remove it from the target_columns
        # (as the condition has been matched). If it is not removed, the pruning algorithm may keep the unused
        # assign operation that generate the same column name.
        target_columns = target_columns.difference(target_node.get_new_output_columns())

        # Update target_columns to include list of required columns for the current node operations
        target_columns.update(target_node.get_required_input_columns())

        # reverse topological sort to make sure "target_columns" get filled properly. Example:
        # edges = {"assign_1": ["groupby_1", "project_1"], "project_1", ["groupby_1"], ...}
        # Here, "groupby_1" node have 2 input_node_names ("assign_1" and "project_1"), reverse topological
        # sort makes sure we travel "project_1" first (filled up target_columns) and then travel assign node.
        # If the assign node's new columns are not in "target_columns", we can safely remove the node.
        for input_node_name in sorted(
            input_node_names, key=lambda x: topological_order_map[x], reverse=True
        ):
            pruned_graph = cls._prune(
                graph=graph,
                target_node=graph.nodes_map[input_node_name],
                target_columns=target_columns,
                pruned_graph=pruned_graph,
                processed_node_names=processed_node_names,
                node_name_map=node_name_map,
                topological_order_map=topological_order_map,
            )

        if to_prune_target_node:
            # do not add the target_node to the pruned graph
            return pruned_graph

        # reconstruction: process the node from the input node towards the target node
        mapped_input_node_names = []
        for input_node_name in input_node_names:
            # if the input node get pruned, it will not exist in the processed_node_names.
            # in this case, keep finding the first parent node exists in the processed_node_names.
            # currently only ASSIGN node could get pruned, the first input node is the frame node.
            # it is used to replace the pruned assigned node
            while input_node_name not in processed_node_names:
                input_node_name = graph.backward_edges_map[input_node_name][0]
            mapped_input_node_names.append(input_node_name)

        # add the node back to the pruned graph
        input_nodes = [
            pruned_graph.nodes_map[node_name_map[node_name]]
            for node_name in mapped_input_node_names
        ]
        node_pruned = pruned_graph.add_operation(
            node_type=NodeType(target_node.type),
            node_params=target_node.parameters.dict(),
            node_output_type=NodeOutputType(target_node.output_type),
            input_nodes=input_nodes,
        )

        # update the container to store the mapped node name & processed nodes information
        node_name_map[target_node.name] = node_pruned.name
        processed_node_names.add(target_node.name)
        return pruned_graph

    @classmethod
    def prune(
        cls,
        graph: QueryGraphT,
        output_graph: QueryGraphT,
        target_node: Node,
        target_columns: Set[str],
    ) -> Tuple[QueryGraphT, Dict[str, str]]:
        """
        Prune the query graph and return the pruned graph & mapped node.

        To prune the graph, this function first traverses from the target node to the input node.
        The unused branches of the graph will get pruned in this step. After that, a new graph is
        reconstructed by adding the required nodes back.

        Parameters
        ----------
        graph: QueryGraphT
            Graph to be pruned
        output_graph: QueryGraphT
            Output graph used to store pruned graph
        target_node: Node
            target end node
        target_columns: set[str]
            list of target columns

        Returns
        -------
        QueryGraph, node_name_map
        """
        node_name_map: Dict[str, str] = {}
        output_graph = cls._prune(
            graph=graph,
            target_node=target_node,
            target_columns=target_columns,
            pruned_graph=output_graph,
            processed_node_names=set(),
            node_name_map=node_name_map,
            topological_order_map=graph.node_topological_order_map,
        )
        return output_graph, node_name_map


class GraphReconstructor:
    """GraphReconstructor class"""

    @classmethod
    def add_groupby_operation(
        cls,
        graph: QueryGraphT,
        node_cls: Type[NodeT],
        node_params: Dict[str, Any],
        input_node: Node,
    ) -> NodeT:
        """
        Insert groupby operation

        Parameters
        ----------
        graph: QueryGraphT
            Query graph
        node_params: Dict[str, Any]
            Node parameters
        input_node: Node
            Input node

        Returns
        -------
        NodeT

        Raises
        ------
        ValueError
            When the query graph have unexpected structure (groupby node should have at least an InputNode)
        """
        # create a temporary groupby node & prune the graph to generate tile_id & aggregation_id
        temp_node = node_cls(name="temp", parameters=node_params)
        pruned_graph, node_name_map = GraphPruner.prune(
            graph=cast(QueryGraphModel, graph),
            output_graph=QueryGraphModel(),
            target_node=input_node,
            target_columns=set(temp_node.get_required_input_columns()),
        )
        assert isinstance(pruned_graph, QueryGraphModel)
        pruned_input_node_name = None
        for node in dfs_traversal(graph, input_node):
            if pruned_input_node_name is None and node.name in node_name_map:
                # as the input node could be pruned in the pruned graph, traverse the graph to find a valid input
                pruned_input_node_name = node_name_map[node.name]

        if pruned_input_node_name is None:
            raise ValueError("Failed to add groupby operation.")

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
        return cast(NodeT, node)

    # @classmethod
    # def add_groupby_operation(
    #     cls, graph: QueryGraphT, node_params: Dict[str, Any], input_node: Node
    # ) -> GroupbyNode:
    #     """
    #     Insert groupby operation
    #
    #     Parameters
    #     ----------
    #     graph: QueryGraphT
    #         Query graph
    #     node_params: Dict[str, Any]
    #         Node parameters
    #     input_node: Node
    #         Input node
    #
    #     Returns
    #     -------
    #     GroupbyNode
    #
    #     Raises
    #     ------
    #     ValueError
    #         When the query graph have unexpected structure (groupby node should have at least an InputNode)
    #     """
    #     # create a temporary groupby node & prune the graph to generate tile_id & aggregation_id
    #     temp_node = GroupbyNode(
    #         name="temp", parameters=node_params, output_type=NodeOutputType.FRAME
    #     )
    #     pruned_graph, node_name_map = GraphPruner.prune(
    #         graph=cast(QueryGraphModel, graph),
    #         output_graph=QueryGraphModel(),
    #         target_node=input_node,
    #         target_columns=set(temp_node.get_required_input_columns()),
    #     )
    #     assert isinstance(pruned_graph, QueryGraphModel)
    #     pruned_input_node_name = None
    #     table_details = None
    #     for node in dfs_traversal(graph, input_node):
    #         if pruned_input_node_name is None and node.name in node_name_map:
    #             # as the input node could be pruned in the pruned graph, traverse the graph to find a valid input
    #             pruned_input_node_name = node_name_map[node.name]
    #         if isinstance(node, InputNode) and node.parameters.type == TableDataType.EVENT_DATA:
    #             # get the table details from the input node
    #             table_details = node.parameters.table_details.dict()
    #             break
    #
    #     if pruned_input_node_name is None or table_details is None:
    #         raise ValueError("Failed to add groupby operation.")
    #
    #     # tile_id & aggregation_id should be based on pruned graph to improve tile reuse
    #     tile_id = get_tile_table_identifier(
    #         table_details_dict=table_details, parameters=temp_node.parameters.dict()
    #     )
    #     aggregation_id = get_aggregation_identifier(
    #         transformations_hash=pruned_graph.node_name_to_ref[pruned_input_node_name],
    #         parameters=temp_node.parameters.dict(),
    #     )
    #
    #     # insert the groupby node using the generated tile_id & aggregation_id
    #     node = graph.add_operation(
    #         node_type=NodeType.GROUPBY,
    #         node_params={
    #             **temp_node.parameters.dict(),
    #             "tile_id": tile_id,
    #             "aggregation_id": aggregation_id,
    #         },
    #         node_output_type=NodeOutputType.FRAME,
    #         input_nodes=[input_node],
    #     )
    #     return cast(GroupbyNode, node)

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
                cls.add_groupby_operation(
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
