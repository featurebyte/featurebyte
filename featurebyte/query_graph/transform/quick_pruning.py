"""
This module contains graph quick pruning related classes.
"""

from typing import Any, Dict, List, Set

from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.model.graph import GraphNodeNameMap, QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.transform.base import BaseGraphTransformer


class QuickGraphPruningGlobalState(FeatureByteBaseModel):
    """QuickGraphPruningGlobalState class"""

    graph: QueryGraphModel = Field(default_factory=QueryGraphModel)
    node_name_map: Dict[str, str] = Field(default_factory=dict)

    # node_names_to_keep is used to specify the graph node names we want to keep.
    node_names_to_keep: Set[str]


class QuickGraphStructurePruningTransformer(
    BaseGraphTransformer[GraphNodeNameMap, QuickGraphPruningGlobalState]
):
    """QuickGraphStructurePruningTransformer class"""

    def _compute(self, global_state: QuickGraphPruningGlobalState, node: Node) -> None:
        input_nodes = [
            global_state.graph.get_node_by_name(global_state.node_name_map[input_node_name])
            for input_node_name in self.graph.get_input_node_names(node=node)
        ]
        inserted_node = global_state.graph.add_operation(
            node_type=node.type,
            node_params=node.parameters.model_dump(),
            node_output_type=node.output_type,
            input_nodes=input_nodes,
        )
        global_state.node_name_map[node.name] = inserted_node.name

    @classmethod
    def _extract_node_names_to_keep(
        cls, graph: QueryGraphModel, target_node_names: List[str]
    ) -> Set[str]:
        node_names_to_keep: Set[str] = set()
        for target_node_name in target_node_names:
            for node in graph.iterate_nodes(
                target_node=graph.get_node_by_name(target_node_name), node_type=None
            ):
                node_names_to_keep.add(node.name)
        return node_names_to_keep

    def _transform(self, global_state: QuickGraphPruningGlobalState) -> Any:
        for node_name in self.graph.sorted_node_names:
            if node_name in global_state.node_names_to_keep:
                self._compute(
                    global_state=global_state, node=self.graph.get_node_by_name(node_name)
                )

    def transform(self, target_node_names: List[str]) -> GraphNodeNameMap:
        """
        Transform the graph by pruning the graph to only contain the target nodes and their
        dependencies without modifying existing node parameters.

        Parameters
        ----------
        target_node_names: List[str]
            The target node names to keep in the graph.

        Returns
        -------
        GraphNodeNameMap
        """
        node_names_to_keep = self._extract_node_names_to_keep(
            graph=self.graph, target_node_names=target_node_names
        )
        global_state = QuickGraphPruningGlobalState(node_names_to_keep=node_names_to_keep)
        self._transform(global_state=global_state)
        return global_state.graph, global_state.node_name_map
