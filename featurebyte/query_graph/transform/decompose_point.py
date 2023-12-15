"""
This module contains
"""
from typing import Dict, List, Optional, Set

from dataclasses import dataclass

from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.entity_relationship_info import (
    EntityAncestorDescendantMapper,
    EntityRelationshipInfo,
)
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import LookupNode, LookupTargetNode
from featurebyte.query_graph.node.mixin import AggregationOpStructMixin, BaseGroupbyParameters
from featurebyte.query_graph.node.request import RequestColumnNode
from featurebyte.query_graph.transform.base import BaseGraphTransformer
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor


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

    agg_node_types: List[NodeType]
    primary_entity_ids: List[PydanticObjectId]
    feature_job_settings: List[FeatureJobSetting]
    has_request_column: bool
    has_ingest_graph_node: bool

    def __init__(self) -> None:
        self.agg_node_types = []
        self.primary_entity_ids = []
        self.feature_job_settings = []
        self.has_request_column = False
        self.has_ingest_graph_node = False

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
        output.agg_node_types = sorted(set(self.agg_node_types + other.agg_node_types))
        output.primary_entity_ids = sorted(set(self.primary_entity_ids + other.primary_entity_ids))
        output.feature_job_settings = list(
            set(self.feature_job_settings + other.feature_job_settings)
        )
        output.has_request_column = self.has_request_column or other.has_request_column
        output.has_ingest_graph_node = self.has_ingest_graph_node or other.has_ingest_graph_node
        return output

    @property
    def has_ttl_agg_type(self) -> bool:
        """
        Check whether the aggregation info contains time-to-live aggregation type

        Returns
        -------
        bool
        """
        return NodeType.GROUPBY in self.agg_node_types


@dataclass
class DecomposePointGlobalState:
    """DecomposePointGlobalState class stores global state of the decompose point extractor"""

    # entity id to ancestor/descendant mapping
    entity_ancestor_descendant_mapper: EntityAncestorDescendantMapper
    # (original graph) node name to aggregation node info mapping (from the original graph)
    node_name_to_aggregation_info: Dict[str, AggregationInfo]
    # (original graph) aggregation node names used to determine whether to start decomposing the graph
    aggregation_node_names: Set[str]
    # (original graph) node names that should be used as decompose point
    decompose_node_names: Set[str]
    # (original graph) node names that should be used as offline store ingest query graph output node
    ingest_graph_output_node_names: Set[str]

    @property
    def should_decompose(self) -> bool:
        """
        Check whether the graph should be decomposed

        Returns
        -------
        bool
        """
        return bool(self.decompose_node_names)

    @classmethod
    def create(
        cls,
        relationships_info: List[EntityRelationshipInfo],
        aggregation_node_names: Set[str],
    ) -> "DecomposePointGlobalState":
        """
        Create DecomposePointGlobalState object

        Parameters
        ----------
        relationships_info: List[EntityRelationshipInfo]
            List of entity relationship info
        aggregation_node_names: Set[str]
            Set of aggregation node names

        Returns
        -------
        DecomposePointGlobalState
        """
        entity_ancestor_descendant_mapper = EntityAncestorDescendantMapper.create(
            relationships_info=relationships_info
        )
        return cls(
            entity_ancestor_descendant_mapper=entity_ancestor_descendant_mapper,
            node_name_to_aggregation_info={},
            aggregation_node_names=aggregation_node_names,
            decompose_node_names=set(),
            ingest_graph_output_node_names=set(),
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
            aggregation_info.agg_node_types = [node.type]

        if isinstance(node.parameters, BaseGroupbyParameters):
            # primary entity ids introduced by groupby node family
            aggregation_info.primary_entity_ids = node.parameters.entity_ids or []
        elif isinstance(node, (LookupNode, LookupTargetNode)):
            # primary entity ids introduced by lookup node family
            aggregation_info.primary_entity_ids = [node.parameters.entity_id]

        if isinstance(node, RequestColumnNode):
            # request columns introduced by request column node
            aggregation_info.has_request_column = True

        if isinstance(node, AggregationOpStructMixin):
            feature_job_setting = node.extract_feature_job_setting()
            if feature_job_setting:
                aggregation_info.feature_job_settings = [feature_job_setting]

        # reduce the primary entity ids based on entity relationship
        aggregation_info.primary_entity_ids = self.entity_ancestor_descendant_mapper.reduce_entity_ids(
            entity_ids=aggregation_info.primary_entity_ids  # type: ignore
        )

        # update the mapping
        self.node_name_to_aggregation_info[node.name] = aggregation_info

    def check_input_aggregations(
        self, agg_info: AggregationInfo, input_node_names: List[str]
    ) -> bool:
        """
        Check whether to split the input nodes into multiple offline store ingest query graphs

        Parameters
        ----------
        agg_info: AggregationInfo
            Aggregation info of the current node
        input_node_names: List[str]
            List of input node names

        Returns
        -------
        bool
        """
        # check whether the input nodes have mixed request column flags or mixed ingest graph node flags
        input_aggregations_info = [
            self.node_name_to_aggregation_info[input_node_name]
            for input_node_name in input_node_names
        ]

        # Check for conditions where splitting should occur
        split_conditions = []
        for input_agg_info in input_aggregations_info:
            split_conditions.append(
                input_agg_info.has_request_column or input_agg_info.has_ingest_graph_node
            )

        # Check if any of the inputs have either request column or ingest graph node, split it
        if any(split_conditions):
            # If all inputs have either request columns or ingest graph nodes, do not split
            if all(split_conditions):
                return False
            return True

        # check whether the input nodes can be merged into one offline store ingest query graph
        all_inputs_have_empty_agg_node_types = True
        for input_agg_info in input_aggregations_info:
            if (
                input_agg_info.primary_entity_ids == agg_info.primary_entity_ids
                and input_agg_info.feature_job_settings == agg_info.feature_job_settings
                and input_agg_info.has_ttl_agg_type == agg_info.has_ttl_agg_type
            ):
                # if any of the input is the same as the output, that means
                # - no new entity ids are added
                # - no new feature job settings are added
                # - no new time-to-live aggregation type is added
                # to the universe.
                return False

            if input_agg_info.agg_node_types:
                all_inputs_have_empty_agg_node_types = False

        if all_inputs_have_empty_agg_node_types:
            # if all the input nodes do not have any aggregation operation introduced, that means
            # the current node is the first aggregation operation introduced in the graph (between
            # the input nodes and the current node).
            return False

        # if none of the above conditions are met, that means we should split the query graph
        return True

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
        agg_info = self.node_name_to_aggregation_info[node_name]
        if not agg_info.agg_node_types:
            # do not decompose if aggregation operation has not been introduced
            return False

        return self.check_input_aggregations(agg_info=agg_info, input_node_names=input_node_names)

    def update_ingest_graph_node_output_names(self, input_node_names: List[str]) -> None:
        """
        Check the list of input node names to determine whether the input should be an offline store ingest
        query graph. If so, add the input node name to the list of ingest graph output node names.

        Parameters
        ----------
        input_node_names: List[str]
            List of input node names
        """
        input_aggregations_info = [
            self.node_name_to_aggregation_info[input_node_name]
            for input_node_name in input_node_names
        ]
        any_input_has_request_column = False
        any_input_has_ingest_graph_node = False
        for input_agg_info in input_aggregations_info:
            any_input_has_request_column |= input_agg_info.has_request_column
            any_input_has_ingest_graph_node |= input_agg_info.has_ingest_graph_node

        for input_node_name, input_agg_info in zip(input_node_names, input_aggregations_info):
            if input_agg_info.has_request_column:
                # if the input node has request column, it must not be an offline store ingest query graph
                continue

            if input_agg_info.has_ingest_graph_node:
                # if the input node has ingest graph node, it must not be an offline store ingest query graph
                continue

            self.ingest_graph_output_node_names.add(input_node_name)
            self.node_name_to_aggregation_info[input_node_name].has_ingest_graph_node = True

        if not self.ingest_graph_output_node_names.intersection(input_node_names):
            assert False, "No offline store ingest query graph output node found"


class DecomposePointExtractor(
    BaseGraphTransformer[DecomposePointGlobalState, DecomposePointGlobalState]
):
    """
    DecomposePointExtractor class is used to extract information about the decompose point
    used in offline ingest graph decomposition.
    """

    def _compute(
        self,
        global_state: DecomposePointGlobalState,
        node: Node,
    ) -> None:
        input_node_names = self.graph.get_input_node_names(node)
        global_state.update_aggregation_info(node=node, input_node_names=input_node_names)
        to_decompose = global_state.should_decompose_query_graph(
            node_name=node.name, input_node_names=input_node_names
        )
        if to_decompose:
            global_state.decompose_node_names.add(node.name)
            global_state.update_ingest_graph_node_output_names(input_node_names=input_node_names)
            global_state.node_name_to_aggregation_info[node.name].has_ingest_graph_node = True

    def extract(
        self,
        node: Node,
        relationships_info: Optional[List[EntityRelationshipInfo]] = None,
    ) -> DecomposePointGlobalState:
        """
        Extract information about offline store ingest query graph decomposition

        Parameters
        ----------
        node: Node
            Target output node
        relationships_info: Optional[List[EntityRelationshipInfo]]
            List of entity relationship info

        Returns
        -------
        DecomposePointGlobalState
        """
        # identify aggregation node names in the graph, aggregation node names are used to determine
        # whether to start decomposing the graph
        op_struct_state = OperationStructureExtractor(graph=self.graph).extract(node=node)
        op_struct = op_struct_state.operation_structure_map[node.name]
        aggregation_node_names = {agg.node_name for agg in op_struct.iterate_aggregations()}

        global_state = DecomposePointGlobalState.create(
            relationships_info=relationships_info or [],
            aggregation_node_names=aggregation_node_names,
        )
        self._transform(global_state=global_state)
        return global_state
