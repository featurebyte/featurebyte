"""
VersionService class
"""
from __future__ import annotations

from typing import Any, Iterator, Optional, Tuple, cast

from bson.objectid import ObjectId

from featurebyte.enum import TableDataType
from featurebyte.exception import DocumentError
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.models.feature import FeatureModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import GroupbyNode, InputNode
from featurebyte.schema.feature import FeatureCreate, VersionCreate
from featurebyte.service.base_update import BaseUpdateService


class VersionService(BaseUpdateService):
    """
    VersionService class is responsible for creating new feature version
    """

    @staticmethod
    def _iterate_groupby_and_event_data_input_node_pairs(
        graph: QueryGraph, target_node: Node
    ) -> Iterator[Tuple[GroupbyNode, InputNode]]:
        # add a staticmethod here, move it to QueryGraph if it is reused by others
        for groupby_node in graph.iterate_nodes(
            target_node=target_node, node_type=NodeType.GROUPBY
        ):
            event_data_input_node: Optional[InputNode] = None
            for input_node in graph.iterate_nodes(
                target_node=groupby_node, node_type=NodeType.INPUT
            ):
                assert isinstance(input_node, InputNode)
                if input_node.parameters.type == TableDataType.EVENT_DATA:
                    event_data_input_node = input_node
            if event_data_input_node is None:
                raise ValueError("Groupby node does not have valid event data!")
            yield cast(GroupbyNode, groupby_node), event_data_input_node

    @classmethod
    def _create_new_feature_version(
        cls, feature: FeatureModel, feature_job_setting: Optional[FeatureJobSetting]
    ) -> FeatureModel:
        has_change: bool = False
        graph = feature.graph
        if feature_job_setting:
            replace_nodes_map: dict[str, Node] = {}
            for groupby_node, _ in cls._iterate_groupby_and_event_data_input_node_pairs(
                feature.graph, feature.node
            ):
                # input node will be used when we need to support updating specific groupby node given event data ID
                parameters = {**groupby_node.parameters.dict(), **feature_job_setting.to_seconds()}
                if groupby_node.parameters.dict() != parameters:
                    replace_nodes_map[groupby_node.name] = GroupbyNode(
                        **{**groupby_node.dict(), "parameters": parameters}
                    )

            if replace_nodes_map:
                has_change = True
                graph = feature.graph.reconstruct(
                    replace_nodes_map=replace_nodes_map, regenerate_groupby_hash=True
                )

        if has_change:
            return FeatureModel(**{**feature.dict(), "graph": graph, "_id": ObjectId()})
        raise DocumentError("No change detected on the new feature version.")

    async def create_new_feature_version(
        self,
        data: VersionCreate,
        get_credential: Any,
    ) -> FeatureModel:
        """
        Create new feature version based on given source feature

        Parameters
        ----------
        data: VersionCreate
            Version creation payload
        get_credential: Any
            Get credential handler function

        Returns
        -------
        FeatureModel
        """
        feature = await self.feature_service.get_document(document_id=data.source_feature_id)
        new_feature = self._create_new_feature_version(feature, data.feature_job_setting)
        return await self.feature_service.create_document(
            data=FeatureCreate(**new_feature.dict(by_alias=True)), get_credential=get_credential
        )
