"""
This module contains offline store ingest query extraction related classes.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.dtype import DBVarTypeInfo
from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.nested import (
    AggregationNodeInfo,
    OfflineStoreIngestQueryGraphNodeParameters,
)
from featurebyte.query_graph.transform.base import BaseGraphTransformer
from featurebyte.query_graph.transform.decompose_point import (
    AggregationInfo,
    DecomposePointExtractor,
    DecomposePointState,
    FeatureJobSettingExtractor,
)
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor
from featurebyte.query_graph.transform.quick_pruning import QuickGraphStructurePruningTransformer


def extract_dtype_info_from_graph(
    graph: QueryGraphModel, output_node: Node, exception_message: Optional[str] = None
) -> DBVarTypeInfo:
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
    DBVarTypeInfo

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
    return op_struct.aggregations[0].dtype_info


@dataclass
class OfflineStoreIngestQueryGraphGlobalState:
    """OfflineStoreIngestQueryGlobalState class"""

    # decomposed graph output
    graph: QueryGraphModel
    node_name_map: Dict[str, str]

    # variables used to decompose the graph
    target_node_name: str
    decompose_point_info: DecomposePointState

    # variables used to construct offline store table name
    feature_name: str
    feature_version: str
    ingest_graph_node_counter: int

    @classmethod
    def create(
        cls,
        feature_name: str,
        feature_version: str,
        target_node_name: str,
        decompose_point_info: DecomposePointState,
    ) -> "OfflineStoreIngestQueryGraphGlobalState":
        """
        Create a new OfflineStoreIngestQueryGlobalState object from the given relationships info

        Parameters
        ----------
        feature_name: str
            Feature name
        feature_version: str
            Feature version
        target_node_name: str
            Target node name
        decompose_point_info: DecomposePointState
            Decompose point info

        Returns
        -------
        OfflineStoreIngestQueryGraphGlobalState
        """
        return OfflineStoreIngestQueryGraphGlobalState(
            feature_name=feature_name,
            feature_version=feature_version,
            graph=QueryGraphModel(),
            node_name_map={},
            target_node_name=target_node_name,
            ingest_graph_node_counter=0,
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
            node_params=node.parameters.model_dump(by_alias=True),
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
        operation_structure: OperationStructure,
    ) -> Dict[str, Any]:
        agg_nodes_info = []
        feature_job_settings = []
        agg_node_names = [agg.node_name for agg in operation_structure.iterate_aggregations()]
        for node_name in aggregation_node_names:
            if node_name in agg_node_names:
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

                feature_job_setting = FeatureJobSettingExtractor(
                    graph=subgraph
                ).extract_from_agg_node(node=subgraph_agg_node)
                if feature_job_setting:
                    feature_job_settings.append(feature_job_setting)

        assert len(set(feature_job_settings)) <= 1, "Only 1 feature job setting is allowed"
        feature_job_setting = feature_job_settings[0] if feature_job_settings else None
        output_dtype_info = extract_dtype_info_from_graph(
            graph=subgraph, output_node=subgraph_output_node
        )
        parameters = {
            "aggregation_nodes_info": agg_nodes_info,
            "feature_job_setting": feature_job_setting,
            "has_ttl": aggregation_info.has_ttl_agg_type,
            "offline_store_table_name": "",  # will be set later
            "output_dtype_info": output_dtype_info,
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
            operation_structure=global_state.decompose_point_info.operation_structure_map[
                node_name
            ],
        )
        part_num = global_state.ingest_graph_node_counter
        column_name = (
            f"__{global_state.feature_name}_{global_state.feature_version}__part{part_num}"
        )
        graph_node = GraphNode(
            name="graph",
            output_type=subgraph_output_node.output_type,
            parameters=OfflineStoreIngestQueryGraphNodeParameters(
                graph=subgraph,
                output_node_name=subgraph_output_node.name,
                output_column_name=column_name,
                primary_entity_ids=aggregation_info.primary_entity_ids,
                primary_entity_dtypes=aggregation_info.primary_entity_dtypes,
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
        relationships_info: List[EntityRelationshipInfo],
        feature_name: str,
        feature_version: str,
    ) -> OfflineStoreIngestQueryGraphOutput:
        """
        Transform the given node into a decomposed graph with offline store ingest query nodes

        Parameters
        ----------
        target_node: Node
            Output node of the graph
        relationships_info: List[EntityRelationshipInfo]
            Relationships info (used to get the primary entity ids & create the offline store table name)
        feature_name: str
            Feature name (used to create the offline store table column name)
        feature_version: str
            Feature version (used to create the offline store table column name)

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
            feature_version=feature_version,
            target_node_name=target_node.name,
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
