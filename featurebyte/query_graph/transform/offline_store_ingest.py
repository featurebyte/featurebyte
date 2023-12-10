"""
This module contains offline store ingest query extraction related classes.
"""
from typing import Any, Dict, List, Optional, Set

from dataclasses import dataclass

from featurebyte.common.string import sanitize_identifier
from featurebyte.enum import DBVarType
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.mixin import AggregationOpStructMixin
from featurebyte.query_graph.node.nested import (
    AggregationNodeInfo,
    OfflineStoreIngestQueryGraphNodeParameters,
)
from featurebyte.query_graph.transform.base import BaseGraphTransformer
from featurebyte.query_graph.transform.decompose_point import (
    AggregationInfo,
    DecomposePointExtractor,
    DecomposePointGlobalState,
)
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor
from featurebyte.query_graph.transform.quick_pruning import QuickGraphStructurePruningTransformer


def get_offline_store_table_name(
    primary_entity_serving_names: List[str],
    feature_job_setting: Optional[FeatureJobSetting],
    has_ttl: bool,
) -> str:
    """
    Get offline store table name

    Parameters
    ----------
    primary_entity_serving_names: List[str]
        Primary entity serving names
    feature_job_setting: Optional[FeatureJobSetting]
        Feature job setting
    has_ttl: bool
        Whether the offline store table has time-to-live property or not

    Returns
    -------
    str
        Offline store table name
    """
    if primary_entity_serving_names:
        entity_part = "_".join(primary_entity_serving_names)
        table_name = sanitize_identifier(f"fb_entity_{entity_part}")
    else:
        table_name = "fb_entity_overall"

    if feature_job_setting:
        fjs = feature_job_setting.to_seconds()
        frequency = fjs["frequency"]
        time_modulo_frequency = fjs["time_modulo_frequency"]
        blind_spot = fjs["blind_spot"]
        table_name = f"{table_name}_fjs_{frequency}_{time_modulo_frequency}_{blind_spot}"
    if has_ttl:
        table_name = f"{table_name}_ttl"
    return table_name


def extract_dtype_from_graph(
    graph: QueryGraphModel, output_node: Node, exception_message: Optional[str] = None
) -> DBVarType:
    """
    Extract dtype from the given graph and node name

    Parameters
    ----------
    graph: QueryGraphModel
        Query graph
    output_node: Node
        Output node
    exception_message: Optional[str]
        Optional exception message to use if the graph has more than one aggregation output

    Returns
    -------
    DBVarType
        DType

    Raises
    ------
    ValueError
        If the graph has more than one aggregation output
    """
    op_struct_info = OperationStructureExtractor(graph=graph).extract(
        node=output_node,
        keep_all_source_columns=True,
    )
    op_struct = op_struct_info.operation_structure_map[output_node.name]
    if len(op_struct.aggregations) != 1:
        if exception_message is None:
            exception_message = "Graph must have exactly one aggregation output"
        raise ValueError(exception_message)
    return op_struct.aggregations[0].dtype


@dataclass
class OfflineStoreIngestQueryGraphGlobalState:  # pylint: disable=too-many-instance-attributes
    """OfflineStoreIngestQueryGlobalState class"""

    # decomposed graph output
    graph: QueryGraphModel
    node_name_map: Dict[str, str]

    # variables used to decompose the graph
    target_node_name: str
    decompose_point_info: DecomposePointGlobalState

    # variables used to construct offline store table name
    feature_name: str
    entity_id_to_serving_name: Dict[PydanticObjectId, str]
    ingest_graph_node_counter: int

    @classmethod
    def create(
        cls,
        feature_name: str,
        target_node_name: str,
        entity_id_to_serving_name: Dict[PydanticObjectId, str],
        decompose_point_info: DecomposePointGlobalState,
    ) -> "OfflineStoreIngestQueryGraphGlobalState":
        """
        Create a new OfflineStoreIngestQueryGlobalState object from the given relationships info

        Parameters
        ----------
        feature_name: str
            Feature name
        target_node_name: str
            Target node name
        entity_id_to_serving_name: Dict[PydanticObjectId, str]
            Entity id to serving name mapping
        decompose_point_info: DecomposePointGlobalState
            Decompose point info

        Returns
        -------
        OfflineStoreIngestQueryGraphGlobalState
        """
        return OfflineStoreIngestQueryGraphGlobalState(
            feature_name=feature_name,
            graph=QueryGraphModel(),
            node_name_map={},
            target_node_name=target_node_name,
            ingest_graph_node_counter=0,
            entity_id_to_serving_name=entity_id_to_serving_name,
            decompose_point_info=decompose_point_info,
        )

    def add_operation_to_graph(
        self, node: Node, input_nodes: List[Node], original_node_names: Optional[List[str]] = None
    ) -> Node:
        """
        Add operation to the graph

        Parameters
        ----------
        node: Node
            Node to be added
        input_nodes: List[Node]
            List of input nodes
        original_node_names: Optional[List[str]]
            Node names of the original graph that are used to create the given node

        Returns
        -------
        Node
            Added node
        """
        inserted_node = self.graph.add_operation(
            node_type=node.type,
            node_params=node.parameters.dict(by_alias=True),
            node_output_type=node.output_type,
            input_nodes=input_nodes,
        )
        if original_node_names:
            for original_node_name in original_node_names:
                if original_node_name not in self.node_name_map:
                    self.node_name_map[original_node_name] = inserted_node.name
        else:
            self.node_name_map[node.name] = inserted_node.name
        return inserted_node

    def get_mapped_decomposed_graph_node(self, node_name: str) -> Node:
        """
        Get the mapped decomposed graph node for the given node name

        Parameters
        ----------
        node_name: str
            Node name (of the original graph)

        Returns
        -------
        Node
            Decomposed graph node
        """
        return self.graph.get_node_by_name(self.node_name_map[node_name])


@dataclass
class OfflineStoreIngestQueryGraphOutput:
    """OfflineStoreIngestQueryGraphOutput class"""

    graph: QueryGraphModel
    node_name_map: Dict[str, str]
    is_decomposed: bool


class OfflineStoreIngestQueryGraphTransformer(
    BaseGraphTransformer[
        OfflineStoreIngestQueryGraphOutput,
        OfflineStoreIngestQueryGraphGlobalState,
    ]
):
    """
    OfflineStoreIngestQueryGraphTransformer class

    This class is used to decompose a query graph into a query graph with
    - offline store ingest query nested graph nodes
    - post offline store ingest processing nodes
    """

    @staticmethod
    def _prepare_offline_store_ingest_query_specific_node_parameters(
        subgraph: QueryGraphModel,
        subgraph_output_node: Node,
        node_name_to_subgraph_node_name: Dict[str, str],
        aggregation_node_names: Set[str],
        aggregation_info: AggregationInfo,
        entity_id_to_serving_name: Dict[PydanticObjectId, str],
    ) -> Dict[str, Any]:
        agg_nodes_info = []
        feature_job_settings = []
        for node_name in aggregation_node_names:
            if node_name in node_name_to_subgraph_node_name:
                # if the aggregation node is in the subgraph, that means the aggregation node
                # is used to create the offline store ingest query
                subgraph_agg_node_name = node_name_to_subgraph_node_name[node_name]
                subgraph_agg_node = subgraph.get_node_by_name(subgraph_agg_node_name)
                input_node_names = subgraph.get_input_node_names(subgraph_agg_node)
                assert (
                    len(input_node_names) == 1
                ), "All non-request column agg. nodes expect only 1 input node"
                agg_nodes_info.append(
                    AggregationNodeInfo(
                        node_type=subgraph_agg_node.type,
                        node_name=subgraph_agg_node_name,
                        input_node_name=input_node_names[0],
                    )
                )

                if isinstance(subgraph_agg_node, AggregationOpStructMixin):
                    feature_job_setting = subgraph_agg_node.extract_feature_job_setting()
                    if feature_job_setting:
                        feature_job_settings.append(feature_job_setting)

        assert len(set(feature_job_settings)) <= 1, "Only 1 feature job setting is allowed"
        primary_entity_serving_names = [
            entity_id_to_serving_name.get(entity_id, str(entity_id))
            for entity_id in aggregation_info.primary_entity_ids
        ]
        feature_job_setting = feature_job_settings[0] if feature_job_settings else None
        offline_store_table_name = get_offline_store_table_name(
            primary_entity_serving_names=primary_entity_serving_names,
            feature_job_setting=feature_job_setting,
            has_ttl=aggregation_info.has_ttl_agg_type,
        )
        output_dtype = extract_dtype_from_graph(graph=subgraph, output_node=subgraph_output_node)
        parameters = {
            "aggregation_nodes_info": agg_nodes_info,
            "feature_job_setting": feature_job_setting,
            "has_ttl": aggregation_info.has_ttl_agg_type,
            "offline_store_table_name": offline_store_table_name,
            "output_dtype": output_dtype,
        }
        return parameters

    def _insert_offline_store_query_graph_node(
        self, global_state: OfflineStoreIngestQueryGraphGlobalState, node_name: str
    ) -> Node:
        transformer = QuickGraphStructurePruningTransformer(graph=self.graph)
        subgraph, node_name_map = transformer.transform(target_node_names=[node_name])
        subgraph_output_node = subgraph.get_node_by_name(node_name_map[node_name])
        aggregation_info = global_state.decompose_point_info.node_name_to_aggregation_info[
            node_name
        ]
        other_params = self._prepare_offline_store_ingest_query_specific_node_parameters(
            subgraph=subgraph,
            subgraph_output_node=subgraph_output_node,
            node_name_to_subgraph_node_name=node_name_map,
            aggregation_node_names=global_state.decompose_point_info.aggregation_node_names,
            aggregation_info=aggregation_info,
            entity_id_to_serving_name=global_state.entity_id_to_serving_name,
        )
        part_num = global_state.ingest_graph_node_counter
        column_name = f"__{global_state.feature_name}__part{part_num}"
        graph_node = GraphNode(
            name="graph",
            output_type=subgraph_output_node.output_type,
            parameters=OfflineStoreIngestQueryGraphNodeParameters(
                graph=subgraph,
                output_node_name=subgraph_output_node.name,
                output_column_name=column_name,
                primary_entity_ids=aggregation_info.primary_entity_ids,
                **other_params,
            ),
        )
        inserted_node = global_state.add_operation_to_graph(
            node=graph_node,
            input_nodes=[],
            original_node_names=list(node_name_map.keys()),
        )

        # update graph node counter
        global_state.ingest_graph_node_counter += 1
        return inserted_node

    def _compute(self, global_state: OfflineStoreIngestQueryGraphGlobalState, node: Node) -> None:
        decompose_point_info = global_state.decompose_point_info
        if (
            node.name not in decompose_point_info.decompose_node_names
            and node.name != global_state.target_node_name
        ):
            return

        input_node_names = self.graph.get_input_node_names(node)
        decom_input_nodes = []
        for input_node_name in input_node_names:
            if input_node_name in global_state.node_name_map:
                decom_input_nodes.append(
                    global_state.get_mapped_decomposed_graph_node(node_name=input_node_name)
                )
            else:
                if input_node_name in decompose_point_info.ingest_graph_output_node_names:
                    decom_input_nodes.append(
                        self._insert_offline_store_query_graph_node(
                            global_state=global_state, node_name=input_node_name
                        )
                    )
                else:
                    visited_node_names = set()
                    for visited_node in self.graph.iterate_nodes(
                        target_node=self.graph.get_node_by_name(input_node_name),
                        node_type=None,
                        skip_node_names=set(global_state.node_name_map.keys()),
                    ):
                        visited_node_names.add(visited_node.name)

                    for _node in self.graph.iterate_sorted_nodes():
                        if (
                            _node.name in visited_node_names
                            and _node.name not in global_state.node_name_map
                        ):
                            sub_input_nodes = [
                                global_state.get_mapped_decomposed_graph_node(in_node_name)
                                for in_node_name in self.graph.get_input_node_names(node=_node)
                            ]
                            global_state.add_operation_to_graph(
                                node=_node, input_nodes=sub_input_nodes
                            )

                    decom_input_nodes.append(
                        global_state.get_mapped_decomposed_graph_node(node_name=input_node_name)
                    )

        # add current node to the decomposed graph
        global_state.add_operation_to_graph(node=node, input_nodes=decom_input_nodes)

    def transform(
        self,
        target_node: Node,
        entity_id_to_serving_name: Dict[PydanticObjectId, str],
        relationships_info: List[EntityRelationshipInfo],
        feature_name: str,
    ) -> OfflineStoreIngestQueryGraphOutput:
        """
        Transform the given node into a decomposed graph with offline store ingest query nodes

        Parameters
        ----------
        target_node: Node
            Output node of the graph
        entity_id_to_serving_name: Dict[PydanticObjectId, str]
            Entity id to serving name mapping (used to create the offline store table name)
        relationships_info: List[EntityRelationshipInfo]
            Relationships info (used to get the primary entity ids & create the offline store table name)
        feature_name: str
            Feature name (used to create the offline store table name)

        Returns
        -------
        OfflineStoreIngestQueryGraphOutput
        """
        # extract decompose point info
        decompose_point_info = DecomposePointExtractor(graph=self.graph).extract(
            node=target_node, relationships_info=relationships_info
        )

        # create global state
        global_state = OfflineStoreIngestQueryGraphGlobalState.create(
            feature_name=feature_name,
            target_node_name=target_node.name,
            entity_id_to_serving_name=entity_id_to_serving_name,
            decompose_point_info=decompose_point_info,
        )
        if decompose_point_info.should_decompose:
            self._transform(global_state=global_state)

        output = OfflineStoreIngestQueryGraphOutput(
            graph=global_state.graph,
            node_name_map=global_state.node_name_map,
            is_decomposed=global_state.decompose_point_info.should_decompose,
        )
        return output
