"""
This module contains graph reconstruction (by replacing certain nodes) related classes.
"""
from typing import Any, Dict, Type, TypeVar, cast

from abc import abstractmethod

from pydantic import BaseModel, Field

from featurebyte.enum import TableDataType
from featurebyte.query_graph.algorithm import dfs_traversal
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.graph import GraphNodeNameMap, QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.base import BaseNode, NodeT
from featurebyte.query_graph.node.generic import GroupbyNode as BaseGroupbyNode
from featurebyte.query_graph.node.generic import InputNode
from featurebyte.query_graph.node.generic import ItemGroupbyNode as BaseItemGroupbyNode
from featurebyte.query_graph.transform.base import BaseGraphTransformer, QueryGraphT
from featurebyte.query_graph.transform.pruning import GraphPruningExtractor
from featurebyte.query_graph.util import get_aggregation_identifier, get_tile_table_identifier

PRUNING_SENSITIVE_NODE_MAP: Dict[NodeType, Type[BaseNode]] = {}


class BasePruningSensitiveNode(BaseNode):
    """
    Base class for nodes whose parameters have to be determined post pruning

    Nodes with this characteristic (e.g. those that derive some unique id based on its input node)
    should implement this interface.
    """

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        node_type = cls.__fields__["type"].default
        PRUNING_SENSITIVE_NODE_MAP[node_type] = cls

    @classmethod
    @abstractmethod
    def derive_parameters_post_prune(
        cls,
        graph: QueryGraphModel,
        input_node: NodeT,
        temp_node: "BasePruningSensitiveNode",
        pruned_graph: QueryGraphModel,
        pruned_input_node_name: str,
    ) -> Dict[str, Any]:
        """
        Derive additional parameters that should be based on pruned graph

        This method will be called by GraphReconstructionTransformer's add_pruning_sensitive_operation method.

        Parameters
        ----------
        graph: QueryGraphModel
            Query graph before pruning
        input_node: NodeT
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

    @staticmethod
    def _get_table_details(graph: QueryGraphModel, input_node: NodeT) -> Dict[str, Any]:
        """
        Helper method to get table details.

        When there are both EventData and SlowlyChangingDimension, then the feature is being created on an EventView
        joined with SCDView. In that case, the table detail should retrieve the event data's details.

        Parameters
        ----------
        input_node: NodeT
            input node

        Returns
        -------
        Dict[str, Any]
            dict representation of the table details

        Raises
        ------
        ValueError
            raised when we are unable to find the table details
        """
        event_table_details = None
        scd_table_details = None
        valid_table_types = {TableDataType.EVENT_DATA, TableDataType.SCD_DATA}
        for node in dfs_traversal(graph, input_node):
            if isinstance(node, InputNode) and node.parameters.type in valid_table_types:
                # get the table details from the input node
                table_details = node.parameters.table_details.dict()
                if node.parameters.type == TableDataType.EVENT_DATA:
                    event_table_details = table_details
                elif node.parameters.type == TableDataType.SCD_DATA:
                    scd_table_details = table_details

                # minor optimization to early break if we have found both table details
                if scd_table_details is not None and event_table_details is not None:
                    break

        # Error if we are unable to find any table details
        if event_table_details is None and scd_table_details is None:
            raise ValueError("Failed to add groupby operation.")

        if event_table_details is not None:
            return event_table_details
        return scd_table_details

    @classmethod
    def derive_parameters_post_prune(
        cls,
        graph: QueryGraphModel,
        input_node: NodeT,
        temp_node: BasePruningSensitiveNode,
        pruned_graph: QueryGraphModel,
        pruned_input_node_name: str,
    ) -> Dict[str, Any]:
        table_details = GroupbyNode._get_table_details(graph, input_node)
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
        input_node: NodeT,
        temp_node: BasePruningSensitiveNode,
        pruned_graph: QueryGraphModel,
        pruned_input_node_name: str,
    ) -> Dict[str, Any]:
        # TODO: derive aggregation id from input node hash and node parameters
        return {}


PruningSensitiveNodeT = TypeVar("PruningSensitiveNodeT", bound=BasePruningSensitiveNode)


def add_pruning_sensitive_operation(
    graph: QueryGraphT,
    node_cls: Type[PruningSensitiveNodeT],
    node_params: Dict[str, Any],
    input_node: NodeT,
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
    input_node: NodeT
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
        aggressive=True,
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


class GraphReconstructionGlobalState(BaseModel):
    """GraphReconstructionGlobalState class"""

    node_name_to_replacement_node: Dict[str, Node]
    regenerate_groupby_hash: bool

    graph: QueryGraphModel = Field(default_factory=QueryGraphModel)
    node_name_map: Dict[str, str] = Field(
        default_factory=dict
    )  # node_name => reconstructed node_name


class GraphReconstructionTransformer(
    BaseGraphTransformer[GraphNodeNameMap, GraphReconstructionGlobalState]
):
    """GraphReconstructionTransformer class"""

    def _compute(self, global_state: GraphReconstructionGlobalState, node: NodeT) -> None:
        # prepare operation insertion inputs
        node_to_insert = global_state.node_name_to_replacement_node.get(node.name, node)
        input_node_names = self.graph.get_input_node_names(node=node)
        input_nodes = [
            global_state.graph.get_node_by_name(global_state.node_name_map[input_node_name])
            for input_node_name in input_node_names
        ]

        assert node.type == node_to_insert.type
        inserted_node: BaseNode
        if node.type in PRUNING_SENSITIVE_NODE_MAP and global_state.regenerate_groupby_hash:
            inserted_node = add_pruning_sensitive_operation(
                graph=global_state.graph,
                node_cls=PRUNING_SENSITIVE_NODE_MAP[node.type],  # type: ignore
                node_params=node_to_insert.parameters.dict(),
                input_node=input_nodes[0],
            )
        else:
            inserted_node = global_state.graph.add_operation(
                node_type=node.type,
                node_params=node_to_insert.parameters.dict(),
                node_output_type=node_to_insert.output_type,
                input_nodes=input_nodes,
            )

        # update node name mapping between original graph & reconstructed graph
        global_state.node_name_map[node.name] = inserted_node.name

    def transform(
        self, node_name_to_replacement_node: Dict[str, NodeT], regenerate_groupby_hash: bool
    ) -> GraphNodeNameMap:
        """
        Transform the graph by replacing all the nodes specified in the node replacement dictionary
        with an option whether to regenerate the groupby hash value.

        Parameters
        ----------
        node_name_to_replacement_node: Dict[str, NodeT]
            Node replacement dictionary
        regenerate_groupby_hash: bool
            Flag to control whether to regenerate the groupby hash

        Returns
        -------
        Tuple[QueryGraphModel, NodeNameMap]
        """
        global_state = GraphReconstructionGlobalState(
            node_name_to_replacement_node=node_name_to_replacement_node,
            regenerate_groupby_hash=regenerate_groupby_hash,
        )
        self._transform(global_state=global_state)
        return global_state.graph, global_state.node_name_map
