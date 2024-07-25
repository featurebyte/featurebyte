"""
Definition extractor used to extract the definition hash of a query graph.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.transform.base import BaseGraphExtractor

DefinitionHash = str
ColumnNameRemap = Dict[str, str]


@dataclass
class DefinitionHashOutput:
    """
    DefinitionOutput class
    """

    graph: QueryGraphModel
    node_name: str

    @property
    def definition_hash(self) -> DefinitionHash:
        """
        Get the definition hash of the output node

        Returns
        -------
        DefinitionHash
        """
        return self.graph.node_name_to_ref[self.node_name]


class DefinitionGlobalState:
    """DefinitionGlobalState class"""

    def __init__(self) -> None:
        # variables for graph reconstruction
        self.graph = QueryGraphModel()
        self.node_name_map: Dict[str, str] = {}

        # variable to store the column name remap for each node
        self.node_name_to_column_name_remap: Dict[str, ColumnNameRemap] = {}


class DefinitionBranchState:
    """DefinitionBranchState class"""


class DefinitionHashExtractor(
    BaseGraphExtractor[DefinitionHashOutput, DefinitionBranchState, DefinitionGlobalState]
):
    """
    DefinitionHashExtractor class used to extract the definition hash of a query graph. To generate
    the definition hash, we need to do the following and then reconstruct the graph:
    * normalize the user-specified column names by using input node hashes
    * normalize the user-specified column names order by sorting the column names
    * normalize the input node order if the node operation is commutative
    * skip the alias node

    Note: This extractor is expected to be used on a pruned query graph.
    """

    def _pre_compute(
        self,
        branch_state: DefinitionBranchState,
        global_state: DefinitionGlobalState,
        node: Node,
        input_node_names: List[str],
    ) -> Tuple[List[str], bool]:
        return input_node_names, False

    def _in_compute(
        self,
        branch_state: DefinitionBranchState,
        global_state: DefinitionGlobalState,
        node: Node,
        input_node: Node,
    ) -> DefinitionBranchState:
        return DefinitionBranchState()

    def _post_compute(
        self,
        branch_state: DefinitionBranchState,
        global_state: DefinitionGlobalState,
        node: Node,
        inputs: List[Any],
        skip_post: bool,
    ) -> Any:
        if node.name in global_state.node_name_map:
            # this implies that the node has been inserted into the new graph.
            mapped_name = global_state.node_name_map[node.name]
            return global_state.graph.get_node_by_name(mapped_name)

        input_node_names = self.graph.get_input_node_names(node)
        if node.is_commutative:
            input_node_name_pairs = sorted(
                zip(inputs, input_node_names),
                key=lambda x: global_state.graph.node_name_to_ref[x[0].name],
            )
            inputs, input_node_names = [], []
            for input_node, input_node_name in input_node_name_pairs:
                inputs.append(input_node)
                input_node_names.append(input_node_name)

        # normalize & create the node
        input_node_hashes = [
            global_state.graph.node_name_to_ref[input_node.name] for input_node in inputs
        ]
        normalized_node, column_name_remap = node.normalize_and_recreate_node(
            input_node_hashes=input_node_hashes,
            input_node_column_mappings=[
                global_state.node_name_to_column_name_remap[input_node_name]
                for input_node_name in input_node_names
            ],
        )

        # add the node to the graph & update the global state
        if node.type == NodeType.ALIAS:
            # if the node is an alias node, we do not need to add it to the graph, use the input node instead
            mapped_node = inputs[0]
        else:
            mapped_node = global_state.graph.add_operation_node(
                node=normalized_node, input_nodes=inputs
            )

        global_state.node_name_map[node.name] = mapped_node.name
        global_state.node_name_to_column_name_remap[node.name] = column_name_remap
        return mapped_node

    def extract(self, node: Node, **kwargs: Any) -> DefinitionHashOutput:
        global_state = DefinitionGlobalState()
        self._extract(
            node=node,
            branch_state=DefinitionBranchState(),
            global_state=global_state,
            topological_order_map=self.graph.node_topological_order_map,
        )
        mapped_node_name = global_state.node_name_map[node.name]
        output = DefinitionHashOutput(graph=global_state.graph, node_name=mapped_node_name)
        return output
