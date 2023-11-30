"""
This module contains offline store ingest query extraction related classes.
"""
from typing import Any, Dict, List, Optional, Set, Tuple

from collections import defaultdict
from dataclasses import dataclass

from bson import ObjectId

from featurebyte.query_graph.enum import GraphNodeType, NodeType
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.entity_relationship_info import (
    EntityAncestorDescendantMapper,
    EntityRelationshipInfo,
)
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import LookupNode, LookupTargetNode
from featurebyte.query_graph.node.mixin import AggregationOpStructMixin, BaseGroupbyParameters
from featurebyte.query_graph.node.nested import (
    OfflineStoreIngestQueryGraphNodeParameters,
    OfflineStoreRequestColumnQueryGraphNodeParameters,
)
from featurebyte.query_graph.node.request import RequestColumnNode
from featurebyte.query_graph.transform.base import BaseGraphExtractor
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor
from featurebyte.query_graph.transform.quick_pruning import QuickGraphStructurePruningTransformer


@dataclass
class AggregationNodeInfo:
    """
    AggregationNodeInfo class stores information about the aggregation-type node.
    """

    node_type: NodeType
    input_node_name: Optional[str]
    node_name: str


@dataclass
class AggregationInfo:
    """
    AggregationInfo class stores information about the aggregation-type node. Aggregation-type node
    includes
    - GroupByNode (time-to-live)
    - ItemGroupbyNode (non-time-to-live)
    - AggregateAsAtNode (non-time-to-live)
    - LookupNode (SCD lookup, Event lookup & Dimension lookup) (non-time-to-live)
    - RequestColumnNode
    """

    agg_nodes: List[AggregationNodeInfo]
    primary_entity_ids: List[ObjectId]
    feature_job_settings: List[FeatureJobSetting]
    request_columns: List[str]

    def __init__(self) -> None:
        self.agg_nodes = []
        self.primary_entity_ids = []
        self.feature_job_settings = []
        self.request_columns = []

    def __add__(self, other: "AggregationInfo") -> "AggregationInfo":
        """
        Add two AggregationInfo objects

        Parameters
        ----------
        other: AggregationInfo
            Other AggregationInfo object

        Returns
        -------
        AggregationInfo
            Added AggregationInfo object
        """
        output = AggregationInfo()
        output.agg_nodes = sorted(self.agg_nodes + other.agg_nodes, key=lambda node: node.node_name)
        output.primary_entity_ids = sorted(set(self.primary_entity_ids + other.primary_entity_ids))
        output.feature_job_settings = list(
            set(self.feature_job_settings + other.feature_job_settings)
        )
        output.request_columns = sorted(set(self.request_columns + other.request_columns))
        return output

    @property
    def has_ttl_agg_type(self) -> bool:
        """
        Check whether the aggregation info contains time-to-live aggregation type

        Returns
        -------
        bool
        """
        for agg_node in self.agg_nodes:
            if agg_node.node_type == NodeType.GROUPBY:
                return True
        return False


@dataclass
class OfflineStoreIngestQueryGraphGlobalState:  # pylint: disable=too-many-instance-attributes
    """OfflineStoreIngestQueryGlobalState class"""

    # original feature name
    feature_name: str
    # decomposed graph
    graph: QueryGraphModel
    # original graph node name to decomposed graph node name mapping
    node_name_map: Dict[str, str]
    # entity id to ancestor/descendant mapping
    entity_ancestor_descendant_mapper: EntityAncestorDescendantMapper
    # (original graph) node name to aggregation node info mapping
    node_name_to_aggregation_info: Dict[str, AggregationInfo]
    # (decomposed graph) graph node name to the exit node name of the original graph mapping
    # this information is used to construct primary entity ids for the nested graph node
    graph_node_name_to_exit_node_name: Dict[str, str]
    # graph node type counter used to generate non-conflicting feature component suffix
    graph_node_counter: Dict[GraphNodeType, int]
    # aggregation node names used to determine whether to start decomposing the graph
    aggregation_node_names: Set[str]
    # whether the graph is decomposed or not
    is_decomposed: bool = False

    @classmethod
    def create(
        cls,
        relationships_info: Optional[List[EntityRelationshipInfo]],
        feature_name: str,
        aggregation_node_names: Set[str],
    ) -> "OfflineStoreIngestQueryGraphGlobalState":
        """
        Create a new OfflineStoreIngestQueryGlobalState object from the given relationships info

        Parameters
        ----------
        relationships_info: Optional[List[EntityRelationshipInfo]]
            Entity relationship info
        feature_name: str
            Feature name
        aggregation_node_names: Set[str]
            Aggregation node names

        Returns
        -------
        OfflineStoreIngestQueryGraphGlobalState
        """
        return OfflineStoreIngestQueryGraphGlobalState(
            feature_name=feature_name,
            graph=QueryGraphModel(),
            entity_ancestor_descendant_mapper=EntityAncestorDescendantMapper.create(
                relationships_info=relationships_info or []
            ),
            node_name_to_aggregation_info={},
            graph_node_name_to_exit_node_name={},
            graph_node_counter=defaultdict(int),
            node_name_map={},
            aggregation_node_names=aggregation_node_names,
        )

    def update_aggregation_info(self, node: Node, input_node_names: List[str]) -> None:
        """
        Update aggregation info of the given node

        Parameters
        ----------
        node: Node
            Node to be processed
        input_node_names: List[str]
            List of input node names
        """
        aggregation_info = AggregationInfo()
        for input_node_name in input_node_names:
            input_aggregation_info = self.node_name_to_aggregation_info[input_node_name]
            aggregation_info += input_aggregation_info

        if node.name in self.aggregation_node_names:
            assert len(input_node_names) <= 1
            agg_node_info = AggregationNodeInfo(
                node_type=node.type,
                input_node_name=next(iter(input_node_names), None),
                node_name=node.name,
            )
            aggregation_info.agg_nodes = [agg_node_info]

        if isinstance(node.parameters, BaseGroupbyParameters):
            # primary entity ids introduced by groupby node family
            aggregation_info.primary_entity_ids = node.parameters.entity_ids or []  # type: ignore
        elif isinstance(node, (LookupNode, LookupTargetNode)):
            # primary entity ids introduced by lookup node family
            aggregation_info.primary_entity_ids = [node.parameters.entity_id]

        if isinstance(node, RequestColumnNode):
            # request columns introduced by request column node
            aggregation_info.request_columns = [node.parameters.column_name]

        if isinstance(node, AggregationOpStructMixin):
            feature_job_setting = node.extract_feature_job_setting()
            if feature_job_setting:
                # feature job settings introduced by aggregation-type node
                aggregation_info.feature_job_settings = [feature_job_setting]

        # reduce the primary entity ids based on entity relationship
        aggregation_info.primary_entity_ids = (
            self.entity_ancestor_descendant_mapper.reduce_entity_ids(
                entity_ids=aggregation_info.primary_entity_ids
            )
        )

        # update the mapping
        self.node_name_to_aggregation_info[node.name] = aggregation_info

    def should_decompose_query_graph(self, node_name: str, input_node_names: List[str]) -> bool:
        """
        Check whether to decompose the query graph into graph with nested offline store ingest query nodes

        Parameters
        ----------
        node_name: str
            Node name
        input_node_names: List[str]
            List of input node names

        Returns
        -------
        bool
        """
        aggregation_info = self.node_name_to_aggregation_info[node_name]
        if not aggregation_info.agg_nodes:
            # do not decompose if aggregation operation has not been introduced
            return False

        all_inputs_have_empty_agg_node_types = True
        for input_node_name in input_node_names:
            input_agg_info = self.node_name_to_aggregation_info[input_node_name]
            if (
                input_agg_info.primary_entity_ids == aggregation_info.primary_entity_ids
                and input_agg_info.request_columns == aggregation_info.request_columns
                and input_agg_info.feature_job_settings == aggregation_info.feature_job_settings
                and input_agg_info.has_ttl_agg_type == aggregation_info.has_ttl_agg_type
            ):
                # if any of the input is the same as the output, that means
                # - no new entity ids are added
                # - no new request columns are added
                # - no new feature job settings are added
                # - no new time-to-live aggregation type is added
                # to the universe.
                return False

            if input_agg_info.agg_nodes:
                all_inputs_have_empty_agg_node_types = False

        if all_inputs_have_empty_agg_node_types:
            # if all the input nodes do not have any aggregation operation introduced, that means
            # the current node is the first aggregation operation introduced in the graph (between
            # the input nodes and the current node).
            return False

        # if none of the above conditions are met, that means we should split the query graph
        return True

    def add_operation_to_graph(self, node: Node, input_nodes: List[Node]) -> Node:
        """
        Add operation to the graph

        Parameters
        ----------
        node: Node
            Node to be added
        input_nodes: List[Node]
            List of input nodes

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
class OfflineStoreIngestQueryGraphBranchState:
    """OfflineStoreIngestQueryBranchState class"""


class OfflineStoreIngestQueryGraphExtractor(
    BaseGraphExtractor[
        OfflineStoreIngestQueryGraphGlobalState,
        OfflineStoreIngestQueryGraphBranchState,
        OfflineStoreIngestQueryGraphGlobalState,
    ]
):
    """
    OfflineStoreIngestExtractor class

    This class is used to decompose a query graph into a query graph with
    - offline store ingest query nested graph nodes
    - post offline store ingest processing nodes
    """

    def _pre_compute(
        self,
        branch_state: OfflineStoreIngestQueryGraphBranchState,
        global_state: OfflineStoreIngestQueryGraphGlobalState,
        node: Node,
        input_node_names: List[str],
    ) -> Tuple[List[str], bool]:
        return input_node_names, False

    def _in_compute(
        self,
        branch_state: OfflineStoreIngestQueryGraphBranchState,
        global_state: OfflineStoreIngestQueryGraphGlobalState,
        node: Node,
        input_node: Node,
    ) -> OfflineStoreIngestQueryGraphBranchState:
        return branch_state

    def _insert_offline_store_query_graph_node(
        self, global_state: OfflineStoreIngestQueryGraphGlobalState, node_name: str
    ) -> Node:
        """
        Insert offline store ingest query node to the decomposed graph

        Parameters
        ----------
        global_state: OfflineStoreIngestQueryGraphGlobalState
            OfflineStoreIngestQueryGlobalState object
        node_name: str
            Node name of the original graph that is used to create the offline store ingest query node

        Returns
        -------
        Node
            Added node (of the decomposed graph)
        """
        transformer = QuickGraphStructurePruningTransformer(graph=self.graph)
        subgraph, node_name_to_transformed_node_name = transformer.transform(
            target_node_names=[node_name]
        )
        transformed_node = subgraph.get_node_by_name(node_name_to_transformed_node_name[node_name])
        aggregation_info = global_state.node_name_to_aggregation_info[node_name]
        parameter_class: Any
        if aggregation_info.request_columns:
            graph_node_type = GraphNodeType.OFFLINE_STORE_REQUEST_COLUMN_QUERY
            parameter_class = OfflineStoreRequestColumnQueryGraphNodeParameters
            suffix = "__req_part"
        else:
            graph_node_type = GraphNodeType.OFFLINE_STORE_INGEST_QUERY
            parameter_class = OfflineStoreIngestQueryGraphNodeParameters
            suffix = "__part"

        comp_count = global_state.graph_node_counter[graph_node_type]
        column_name = f"__{global_state.feature_name}{suffix}{comp_count}"
        graph_node = GraphNode(
            name="graph",
            output_type=transformed_node.output_type,
            parameters=parameter_class(
                graph=subgraph,
                output_node_name=transformed_node.name,
                output_column_name=column_name,
            ),
        )
        inserted_node = global_state.add_operation_to_graph(node=graph_node, input_nodes=[])

        # update graph node type counter
        global_state.graph_node_counter[graph_node_type] += 1

        # store the graph node name to the exit node name of the original graph mapping
        # this information is used to construct primary entity ids for the nested graph node
        global_state.graph_node_name_to_exit_node_name[inserted_node.name] = node_name
        return inserted_node

    def _post_compute(
        self,
        branch_state: OfflineStoreIngestQueryGraphBranchState,
        global_state: OfflineStoreIngestQueryGraphGlobalState,
        node: Node,
        inputs: List[Any],
        skip_post: bool,
    ) -> None:
        input_node_names = self.graph.get_input_node_names(node)
        global_state.update_aggregation_info(node=node, input_node_names=input_node_names)

        if not global_state.is_decomposed:
            # check whether to decompose the query graph
            to_decompose = global_state.should_decompose_query_graph(
                node_name=node.name, input_node_names=input_node_names
            )
            if to_decompose:
                # insert offline store ingest query node
                decom_input_nodes = []
                for input_node_name in input_node_names:
                    added_node = self._insert_offline_store_query_graph_node(
                        global_state=global_state, node_name=input_node_name
                    )
                    decom_input_nodes.append(added_node)

                # add current node to the decomposed graph
                global_state.add_operation_to_graph(node=node, input_nodes=decom_input_nodes)

            # update global state
            global_state.is_decomposed = to_decompose
        else:
            # if the graph is already decided to be decomposed
            # first, check if any of the input node has its corresponding mapping in the decomposed graph
            has_input_associated_with_decomposed_graph = any(
                input_node_name in global_state.node_name_map
                for input_node_name in input_node_names
            )
            if has_input_associated_with_decomposed_graph:
                # if any of the input node has its corresponding mapping in the decomposed graph
                # that means we should insert the current node to the decomposed graph.
                decom_input_nodes = []
                for input_node_name in input_node_names:
                    if input_node_name in global_state.node_name_map:
                        decom_input_nodes.append(
                            global_state.get_mapped_decomposed_graph_node(node_name=input_node_name)
                        )
                    else:
                        decom_input_nodes.append(
                            self._insert_offline_store_query_graph_node(
                                global_state=global_state, node_name=input_node_name
                            )
                        )

                # add current node to the decomposed graph
                global_state.add_operation_to_graph(node=node, input_nodes=decom_input_nodes)

    def extract(
        self,
        node: Node,
        relationships_info: Optional[List[EntityRelationshipInfo]] = None,
        feature_name: str = "feature",
        **kwargs: Any,
    ) -> OfflineStoreIngestQueryGraphGlobalState:
        # identify aggregation node names in the graph, aggregation node names are used to determine
        # whether to start decomposing the graph
        op_struct_state = OperationStructureExtractor(graph=self.graph).extract(node=node)
        op_struct = op_struct_state.operation_structure_map[node.name]
        aggregation_node_names = {agg.node_name for agg in op_struct.iterate_aggregations()}

        # create global state
        global_state = OfflineStoreIngestQueryGraphGlobalState.create(
            relationships_info=relationships_info,
            feature_name=feature_name,
            aggregation_node_names=aggregation_node_names,
        )
        self._extract(
            node=node,
            branch_state=OfflineStoreIngestQueryGraphBranchState(),
            global_state=global_state,
            topological_order_map=self.graph.node_topological_order_map,
        )
        return global_state
