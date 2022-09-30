"""
VersionService class
"""
from __future__ import annotations

from typing import Any

from bson.objectid import ObjectId

from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.models.feature import FeatureModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.generic import GroupbyNode
from featurebyte.schema.feature import FeatureCreate, VersionCreate
from featurebyte.service.base_update import BaseUpdateService


class VersionService(BaseUpdateService):
    """
    VersionService class is responsible for creating new feature version
    """

    @staticmethod
    def _create_new_feature_version(
        feature: FeatureModel, feature_job_setting: FeatureJobSetting
    ) -> FeatureModel:
        replace_nodes_map = {}
        for groupby_node in feature.graph.iterate_nodes(
            target_node=feature.node, node_type=NodeType.GROUPBY
        ):
            parameters = {**groupby_node.parameters.dict(), **feature_job_setting.to_seconds()}
            replace_nodes_map[groupby_node.name] = GroupbyNode(
                **{**groupby_node.dict(), "parameters": parameters}
            )

        graph = feature.graph.reconstruct(replace_nodes_map=replace_nodes_map, regenerate_hash=True)
        new_feature = FeatureModel(**{**feature.dict(), "graph": graph, "_id": ObjectId()})
        return new_feature

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
