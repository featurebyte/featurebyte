"""
This module contains
"""
from typing import Any, Dict, List, Optional, Set, Tuple

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
from featurebyte.query_graph.transform.base import BaseGraphExtractor
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
    request_columns: List[str]

    def __init__(self) -> None:
        self.agg_node_types = []
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
        output.agg_node_types = sorted(set(self.agg_node_types + other.agg_node_types))
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
            aggregation_info.request_columns = [node.parameters.column_name]

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

        all_inputs_have_empty_agg_node_types = True
        for input_node_name in input_node_names:
            input_agg_info = self.node_name_to_aggregation_info[input_node_name]
            if (
                input_agg_info.primary_entity_ids == agg_info.primary_entity_ids
                and bool(input_agg_info.request_columns) == bool(agg_info.request_columns)
                and input_agg_info.feature_job_settings == agg_info.feature_job_settings
                and input_agg_info.has_ttl_agg_type == agg_info.has_ttl_agg_type
            ):
                # if any of the input is the same as the output, that means
                # - no new entity ids are added
                # - no change in request columns (either both are empty or both are non-empty)
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
        common_primary_entity_ids = set.intersection(
            *[set(info.primary_entity_ids) for info in input_aggregations_info]
        )
        common_feature_job_settings = set.intersection(
            *[set(info.feature_job_settings) for info in input_aggregations_info]
        )
        common_agg_node_types = set.intersection(
            *[set(info.agg_node_types) for info in input_aggregations_info]
        )
        for input_node_name in input_node_names:
            input_agg_info = self.node_name_to_aggregation_info[input_node_name]
            if input_agg_info.request_columns:
                continue
            if set(input_agg_info.primary_entity_ids) != common_primary_entity_ids:
                self.ingest_graph_output_node_names.add(input_node_name)
            if set(input_agg_info.feature_job_settings) != common_feature_job_settings:
                self.ingest_graph_output_node_names.add(input_node_name)
            if set(input_agg_info.agg_node_types) != common_agg_node_types:
                self.ingest_graph_output_node_names.add(input_node_name)


@dataclass
class DecomposePointBranchState:
    """DecomposePointBranchState class stores branch state of the decompose point extractor"""


class DecomposePointExtractor(
    BaseGraphExtractor[
        DecomposePointGlobalState,
        DecomposePointBranchState,
        DecomposePointGlobalState,
    ]
):
    """
    DecomposePointExtractor class is used to extract information about the decompose point
    used in offline ingest graph decomposition.
    """

    def _pre_compute(
        self,
        branch_state: DecomposePointBranchState,
        global_state: DecomposePointGlobalState,
        node: Node,
        input_node_names: List[str],
    ) -> Tuple[List[str], bool]:
        return input_node_names, False

    def _in_compute(
        self,
        branch_state: DecomposePointBranchState,
        global_state: DecomposePointGlobalState,
        node: Node,
        input_node: Node,
    ) -> DecomposePointBranchState:
        return branch_state

    def _post_compute(
        self,
        branch_state: DecomposePointBranchState,
        global_state: DecomposePointGlobalState,
        node: Node,
        inputs: List[Any],
        skip_post: bool,
    ) -> None:
        input_node_names = self.graph.get_input_node_names(node)
        global_state.update_aggregation_info(node=node, input_node_names=input_node_names)
        to_decompose = global_state.should_decompose_query_graph(
            node_name=node.name, input_node_names=input_node_names
        )
        if to_decompose:
            global_state.decompose_node_names.add(node.name)
            global_state.update_ingest_graph_node_output_names(input_node_names=input_node_names)

    def extract(
        self,
        node: Node,
        relationships_info: Optional[List[EntityRelationshipInfo]] = None,
        **kwargs: Any,
    ) -> DecomposePointGlobalState:
        # identify aggregation node names in the graph, aggregation node names are used to determine
        # whether to start decomposing the graph
        op_struct_state = OperationStructureExtractor(graph=self.graph).extract(node=node)
        op_struct = op_struct_state.operation_structure_map[node.name]
        aggregation_node_names = {agg.node_name for agg in op_struct.iterate_aggregations()}

        global_state = DecomposePointGlobalState.create(
            relationships_info=relationships_info or [],
            aggregation_node_names=aggregation_node_names,
        )
        self._extract(
            node=node,
            branch_state=DecomposePointBranchState(),
            global_state=global_state,
            topological_order_map=self.graph.node_topological_order_map,
        )
        return global_state
