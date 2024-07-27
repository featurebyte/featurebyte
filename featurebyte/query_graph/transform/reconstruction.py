"""
This module contains graph reconstruction (by replacing certain nodes) related classes.
"""

from abc import abstractmethod
from typing import Any, Dict, Optional, Type, TypeVar, cast

from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.graph import GraphNodeNameMap, QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.base import BaseNode, NodeT
from featurebyte.query_graph.node.generic import GroupByNode as BaseGroupbyNode
from featurebyte.query_graph.node.generic import ItemGroupbyNode as BaseItemGroupbyNode
from featurebyte.query_graph.node.metadata.operation import OperationStructureInfo
from featurebyte.query_graph.transform.base import BaseGraphTransformer, QueryGraphT
from featurebyte.query_graph.transform.flattening import GraphFlatteningTransformer
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor
from featurebyte.query_graph.transform.pruning import prune_query_graph
from featurebyte.query_graph.util import (
    get_aggregation_identifier,
    get_tile_table_identifier_v1,
    get_tile_table_identifier_v2,
)

PRUNING_SENSITIVE_NODE_MAP: Dict[NodeType, Type[BaseNode]] = {}


class BasePruningSensitiveNode(BaseNode):
    """
    Base class for nodes whose parameters have to be determined post pruning

    Nodes with this characteristic (e.g. those that derive some unique id based on its input node)
    should implement this interface.
    """

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__()
        node_type = cls.model_fields["type"].default
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


class GroupByNode(BaseGroupbyNode, BasePruningSensitiveNode):
    """An extended GroupbyNode that implements the derive_parameters_post_prune method"""

    @staticmethod
    def _get_row_index_lineage_hash(
        pruned_graph: QueryGraphModel,
        pruned_input_node_name: str,
    ) -> str:
        op_struct = (
            OperationStructureExtractor(pruned_graph)
            .extract(node=pruned_graph.get_node_by_name(pruned_input_node_name))
            .operation_structure_map[pruned_input_node_name]
        )
        return pruned_graph.node_name_to_ref[op_struct.row_index_lineage[-1]]

    @classmethod
    def derive_parameters_post_prune(
        cls,
        graph: QueryGraphModel,
        input_node: NodeT,
        temp_node: BasePruningSensitiveNode,
        pruned_graph: QueryGraphModel,
        pruned_input_node_name: str,
    ) -> Dict[str, Any]:
        # tile_id & aggregation_id should be based on pruned graph to improve tile reuse
        row_index_lineage_hash = cls._get_row_index_lineage_hash(
            pruned_graph=pruned_graph, pruned_input_node_name=pruned_input_node_name
        )
        parameters_dict = temp_node.parameters.model_dump()
        tile_id_version = parameters_dict.pop("tile_id_version")
        if tile_id_version == 1:
            tile_id = get_tile_table_identifier_v1(
                row_index_lineage_hash=row_index_lineage_hash,
                parameters=parameters_dict,
            )
        else:
            tile_id = get_tile_table_identifier_v2(
                transformations_hash=pruned_graph.node_name_to_ref[pruned_input_node_name],
                parameters=parameters_dict,
            )
        aggregation_id = get_aggregation_identifier(
            transformations_hash=pruned_graph.node_name_to_ref[pruned_input_node_name],
            parameters=parameters_dict,
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
    operation_structure_info: Optional[OperationStructureInfo] = None,
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
        Input node to the aggregation node
    operation_structure_info: Optional[OperationStructureInfo]
        Operation structure information

    Returns
    -------
    PruningSensitiveNodeT
    """
    if operation_structure_info is None:
        operation_structure_info = OperationStructureExtractor(graph=graph).extract(node=input_node)

    # prepare input operation structure to extract available column names
    input_operation_structure = operation_structure_info.operation_structure_map[input_node.name]

    # create a temporary node & prune the graph before deriving additional parameters based on
    # the pruned graph
    temp_node = node_cls(name="temp", parameters=node_params)
    pruned_graph, _, pruned_input_node_name = prune_query_graph(
        graph=graph,
        node=input_node,
        target_columns=list(
            temp_node.get_required_input_columns(
                input_index=0,
                available_column_names=input_operation_structure.output_column_names,
            )
        ),
        operation_structure_info=operation_structure_info,
    )

    # flatten the pruned graph before further operations
    flat_graph, node_name_map = GraphFlatteningTransformer(graph=pruned_graph).transform()
    mapped_input_node_name = node_name_map[pruned_input_node_name]
    additional_parameters = node_cls.derive_parameters_post_prune(
        graph=graph,
        input_node=input_node,
        temp_node=temp_node,
        pruned_graph=flat_graph,
        pruned_input_node_name=mapped_input_node_name,
    )

    # insert the node by including the derived parameters (e.g. tile_id, aggregation_id, etc)
    node = graph.add_operation(
        node_type=temp_node.type,
        node_params={
            **temp_node.parameters.model_dump(),
            **additional_parameters,
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_node],
    )
    return cast(PruningSensitiveNodeT, node)


class GraphReconstructionGlobalState(FeatureByteBaseModel):
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
                node_params=node_to_insert.parameters.model_dump(),
                input_node=input_nodes[0],
            )
        else:
            inserted_node = global_state.graph.add_operation_node(
                node=node_to_insert, input_nodes=input_nodes
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
