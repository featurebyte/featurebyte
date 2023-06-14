"""
This module contains graph cropping related classes.
"""
from typing import Dict, List, Set

from pydantic import BaseModel, Field

from featurebyte.query_graph.model.graph import GraphCroppingOutput, QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.transform.base import BaseGraphTransformer


class GraphCroppingGlobalState(BaseModel):
    """GraphCroppingGlobalState class"""

    graph: QueryGraphModel = Field(default_factory=QueryGraphModel)
    node_name_map: Dict[str, str] = Field(default_factory=dict)

    # node_names_to_keep is used to specify the graph node names we want to keep.
    node_names_to_keep: Set[str]


class GraphCroppingTransformer(BaseGraphTransformer[GraphCroppingOutput, GraphCroppingGlobalState]):
    """GraphCroppingTransformer class"""

    def _compute(self, global_state: GraphCroppingGlobalState, node: Node) -> None:
        if node.name in global_state.node_names_to_keep:
            input_nodes = [
                global_state.graph.get_node_by_name(global_state.node_name_map[input_node_name])
                for input_node_name in self.graph.get_input_node_names(node=node)
            ]
            inserted_node = global_state.graph.add_operation(
                node_type=node.type,
                node_params=node.parameters.dict(),
                node_output_type=node.output_type,
                input_nodes=input_nodes,
            )
            global_state.node_name_map[node.name] = inserted_node.name

    @staticmethod
    def _dfs_traversal(
        graph: QueryGraphModel, node_name: str, node_names_to_keep: Set[str]
    ) -> None:
        if node_name in node_names_to_keep:
            return
        node_names_to_keep.add(node_name)
        for input_node_name in graph.backward_edges_map[node_name]:
            GraphCroppingTransformer._dfs_traversal(
                graph=graph, node_name=input_node_name, node_names_to_keep=node_names_to_keep
            )

    @classmethod
    def _extract_node_names_to_keep(
        cls, graph: QueryGraphModel, target_node_names: List[str]
    ) -> Set[str]:
        node_names_to_keep: Set[str] = set()
        for node_name in target_node_names:
            cls._dfs_traversal(
                graph=graph, node_name=node_name, node_names_to_keep=node_names_to_keep
            )
        return node_names_to_keep

    def transform(self, target_node_names: List[str]) -> GraphCroppingOutput:
        """
        Transform the graph by cropping the graph to only contain the target nodes and their
        dependencies.

        Parameters
        ----------
        target_node_names: List[str]
            The target node names to keep in the graph.

        Returns
        -------
        GraphCroppingOutput
        """
        node_names_to_keep = self._extract_node_names_to_keep(
            graph=self.graph, target_node_names=target_node_names
        )
        global_state = GraphCroppingGlobalState(node_names_to_keep=node_names_to_keep)
        self._transform(global_state=global_state)
        mapped_target_node_names = [
            global_state.node_name_map[target_node_name] for target_node_name in target_node_names
        ]
        return global_state.graph, mapped_target_node_names
