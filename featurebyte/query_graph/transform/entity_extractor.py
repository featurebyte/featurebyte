"""
This module contains graph entity extraction related classes.
"""
from typing import Any, List, Set, Tuple

from dataclasses import dataclass, field

from bson import ObjectId

from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import LookupNode, LookupTargetNode
from featurebyte.query_graph.node.mixin import BaseGroupbyParameters
from featurebyte.query_graph.transform.base import BaseGraphExtractor


class EntityExtractorBranchState:
    """EntityExtractorBranchState class"""


@dataclass
class EntityExtractorGlobalState:
    """EntityExtractorGlobalState class"""

    entity_ids: Set[ObjectId] = field(default_factory=set)


class EntityExtractor(
    BaseGraphExtractor[
        EntityExtractorGlobalState, EntityExtractorBranchState, EntityExtractorGlobalState
    ]
):
    """EntityExtractor class"""

    def _pre_compute(
        self,
        branch_state: EntityExtractorBranchState,
        global_state: EntityExtractorGlobalState,
        node: Node,
        input_node_names: List[str],
    ) -> Tuple[List[str], bool]:
        skip_input_nodes = False
        if isinstance(node.parameters, BaseGroupbyParameters):
            # if groupby node has entity_ids, skip further exploration on input nodes
            if node.parameters.entity_ids:
                global_state.entity_ids.update(node.parameters.entity_ids)
            skip_input_nodes = True
        elif isinstance(node, (LookupNode, LookupTargetNode)):
            global_state.entity_ids.add(node.parameters.entity_id)
        return [] if skip_input_nodes else input_node_names, False

    def _in_compute(
        self,
        branch_state: EntityExtractorBranchState,
        global_state: EntityExtractorGlobalState,
        node: Node,
        input_node: Node,
    ) -> EntityExtractorBranchState:
        return branch_state

    def _post_compute(
        self,
        branch_state: EntityExtractorBranchState,
        global_state: EntityExtractorGlobalState,
        node: Node,
        inputs: List[Any],
        skip_post: bool,
    ) -> EntityExtractorGlobalState:
        return global_state

    def extract(self, node: Node, **kwargs: Any) -> EntityExtractorGlobalState:
        global_state = EntityExtractorGlobalState()
        self._extract(
            node=node,
            branch_state=EntityExtractorBranchState(),
            global_state=global_state,
            topological_order_map=self.graph.node_topological_order_map,
        )
        return global_state
