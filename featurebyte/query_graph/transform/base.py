"""
This module contains base class used in query graph transform directory.
"""
from typing import Any, Dict, Generic, List, Tuple, TypeVar

from abc import abstractmethod

from featurebyte.query_graph.model import QueryGraphModel
from featurebyte.query_graph.node import Node

ExtractorOutputT = TypeVar("ExtractorOutputT")
BranchStateT = TypeVar("BranchStateT")
GlobalStateT = TypeVar("GlobalStateT")
QueryGraphT = TypeVar("QueryGraphT", bound=QueryGraphModel)


class BaseGraphExtractor(Generic[ExtractorOutputT, BranchStateT, GlobalStateT]):
    """BaseGraphExtractor class"""

    # pylint: disable=too-few-public-methods

    def __init__(self, graph: QueryGraphT):
        self.graph = graph

    @abstractmethod
    def _pre_compute(
        self,
        branch_state: BranchStateT,
        global_state: GlobalStateT,
        node: Node,
        input_node_names: List[str],
    ) -> Tuple[List[str], bool]:
        """
        Computation step before input node traversal & return list of input nodes to traverse

        Parameters
        ----------
        branch_state: BranchStateT
            Branch state
        global_state: GlobalStateT
            Global state
        node: Node
            Node to be traversed
        input_node_names: List[str]
            List of input node names

        Returns
        -------
        Tuple[List[str], bool]
        """

    @abstractmethod
    def _in_compute(
        self, branch_state: BranchStateT, global_state: GlobalStateT, node: Node, input_node: Node
    ) -> BranchStateT:
        """
        Computation step during traversing input nodes

        Parameters
        ----------
        branch_state: BranchStateT
            Branch state
        global_state: GlobalStateT
            Global state
        node: Node
            Current node
        input_node: Node
            Input node to the current node

        Returns
        -------
        BranchStateT
        """

    @abstractmethod
    def _post_compute(
        self,
        branch_state: BranchStateT,
        global_state: GlobalStateT,
        node: Node,
        inputs: List[Any],
        skip_post: bool,
    ) -> Any:
        """
        Computation state after traversing input nodes

        Parameters
        ----------
        branch_state: BranchStateT
            Branch state
        global_state: GlobalStateT
            Global state
        node: Node
            Node used to compute
        inputs: List[Any]
            List of extractor outputs from the input nodes
        skip_post: bool
            Whether to skip post computation

        Returns
        -------
        Any
        """

    def _extract(
        self,
        node: Node,
        branch_state: BranchStateT,
        global_state: GlobalStateT,
        topological_order_map: Dict[str, int],
    ) -> Any:
        input_node_names, skip_post = self._pre_compute(
            branch_state=branch_state,
            global_state=global_state,
            node=node,
            input_node_names=self.graph.get_input_node_names(node),
        )
        input_node_map: Dict[str, Any] = {}
        for input_node_name in sorted(
            input_node_names, key=lambda x: topological_order_map[x], reverse=True
        ):
            input_node = self.graph.nodes_map[input_node_name]
            branch_state = self._in_compute(
                branch_state=branch_state,
                global_state=global_state,
                node=node,
                input_node=input_node,
            )
            input_node_map[input_node_name] = self._extract(
                node=input_node,
                branch_state=branch_state,
                global_state=global_state,
                topological_order_map=topological_order_map,
            )

        return self._post_compute(
            branch_state=branch_state,
            global_state=global_state,
            node=node,
            inputs=[input_node_map[node_name] for node_name in input_node_names],
            skip_post=skip_post,
        )

    @abstractmethod
    def extract(self, node: Node) -> ExtractorOutputT:
        """
        Extract output of the given node from the given query graph

        Parameters
        ----------
        node: Node
            Target node of the extractor

        Returns
        -------
        ExtractorOutputT
        """
