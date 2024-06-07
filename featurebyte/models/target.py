"""
This module contains Target related models
"""

from typing import Optional

import pymongo
from bson import ObjectId
from pydantic import Field

from featurebyte.common.model_util import parse_duration_string
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature import BaseFeatureModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.generic import ForwardAggregateNode, LookupTargetNode


class TargetModel(BaseFeatureModel):
    """
    Model for Target asset

    id: PydanticObjectId
        Target id of the object
    name: str
        Target name
    dtype: DBVarType
        Variable type of the target
    graph: QueryGraph
        Graph contains steps of transformation to generate the target
    node_name: str
        Node name of the graph which represent the target
    tabular_source: TabularSource
        Tabular source used to construct this target
    version: VersionIdentifier
        Target version
    definition: str
        Target definition
    entity_ids: List[PydanticObjectId]
        Entity IDs used by the target
    table_ids: List[PydanticObjectId]
        Table IDs used by the target
    primary_table_ids: Optional[List[PydanticObjectId]]
        Primary table IDs of the target (auto-derive from graph)
    target_namespace_id: PydanticObjectId
        Target namespace id of the object
    created_at: Optional[datetime]
        Datetime when the Target was first saved
    updated_at: Optional[datetime]
        When the Target get updated
    """

    # ID related fields associated with this target
    target_namespace_id: PydanticObjectId = Field(frozen=True, default_factory=ObjectId)

    def derive_window(self) -> Optional[str]:
        """
        Derive window from the graph, if there are multiple windows, return the largest one.

        Returns
        -------
        Optional[str]
        """
        window_to_durations = {}
        target_node = self.graph.get_node_by_name(self.node_name)
        # Iterate through forward aggregate targets
        for node in self.graph.iterate_nodes(
            target_node=target_node, node_type=NodeType.FORWARD_AGGREGATE
        ):
            assert isinstance(node, ForwardAggregateNode)
            if node.parameters.window:
                duration = parse_duration_string(node.parameters.window)
                window_to_durations[node.parameters.window] = duration

        # Iterate through lookup targets
        for node in self.graph.iterate_nodes(
            target_node=target_node, node_type=NodeType.LOOKUP_TARGET
        ):
            assert isinstance(node, LookupTargetNode)
            if node.parameters.offset:
                duration = parse_duration_string(node.parameters.offset)
                window_to_durations[node.parameters.offset] = duration

        if not window_to_durations:
            return None
        return max(window_to_durations, key=window_to_durations.get)  # type: ignore

    class Settings(BaseFeatureModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "target"
        indexes = BaseFeatureModel.Settings.indexes + [
            pymongo.operations.IndexModel("target_namespace_id"),
        ]
