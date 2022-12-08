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
from featurebyte.models.feature_list import FeatureListModel, FeatureListNewVersionMode
from featurebyte.persistent import Persistent
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import GroupbyNode, InputNode
from featurebyte.schema.feature import FeatureCreate, FeatureNewVersionCreate
from featurebyte.schema.feature_list import FeatureListCreate, FeatureListNewVersionCreate
from featurebyte.service.base_service import BaseService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_namespace import FeatureNamespaceService


class VersionService(BaseService):
    """
    VersionService class is responsible for creating new feature version
    """

    def __init__(self, user: Any, persistent: Persistent):
        super().__init__(user, persistent)
        self.feature_service = FeatureService(user=user, persistent=persistent)
        self.feature_namespace_service = FeatureNamespaceService(user=user, persistent=persistent)
        self.feature_list_service = FeatureListService(user=user, persistent=persistent)
        self.feature_list_namespace_service = FeatureListNamespaceService(
            user=user, persistent=persistent
        )

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
        graph: QueryGraphModel = feature.graph
        if feature_job_setting:
            node_name_to_replacement_node: dict[str, Node] = {}
            for groupby_node, _ in cls._iterate_groupby_and_event_data_input_node_pairs(
                feature.graph, feature.node
            ):
                # input node will be used when we need to support updating specific groupby node given event data ID
                parameters = {**groupby_node.parameters.dict(), **feature_job_setting.to_seconds()}
                if groupby_node.parameters.dict() != parameters:
                    node_name_to_replacement_node[groupby_node.name] = GroupbyNode(
                        **{**groupby_node.dict(), "parameters": parameters}
                    )

            if node_name_to_replacement_node:
                has_change = True
                graph = feature.graph.reconstruct(
                    node_name_to_replacement_node=node_name_to_replacement_node,
                    regenerate_groupby_hash=True,
                )

        if has_change:
            return FeatureModel(**{**feature.dict(), "graph": graph, "_id": ObjectId()})
        raise DocumentError("No change detected on the new feature version.")

    async def create_new_feature_version(
        self,
        data: FeatureNewVersionCreate,
    ) -> FeatureModel:
        """
        Create new feature version based on given source feature

        Parameters
        ----------
        data: FeatureNewVersionCreate
            Version creation payload

        Returns
        -------
        FeatureModel
        """
        feature = await self.feature_service.get_document(document_id=data.source_feature_id)
        new_feature = self._create_new_feature_version(feature, data.feature_job_setting)
        return await self.feature_service.create_document(
            data=FeatureCreate(**new_feature.dict(by_alias=True))
        )

    async def _create_new_feature_list_version(
        self,
        feature_list: FeatureListModel,
        feature_namespaces: list[dict[str, Any]],
        data: FeatureListNewVersionCreate,
    ) -> FeatureListModel:
        feat_name_to_default_id_map = {
            feat_namespace["name"]: feat_namespace["default_feature_id"]
            for feat_namespace in feature_namespaces
        }
        if data.mode == FeatureListNewVersionMode.AUTO:
            # for auto mode, use default feature id for all the features within the feature list
            features = []
            for feature_id in feat_name_to_default_id_map.values():
                features.append(await self.feature_service.get_document(document_id=feature_id))
        else:
            if not data.features:
                raise DocumentError("Feature info is missing.")

            feature_id_to_name_map = {
                feat_id: feature_namespace["name"]
                for feature_namespace in feature_namespaces
                for feat_id in feature_namespace["feature_ids"]
            }
            specified_feature_map = {
                feat_info.name: feat_info.version for feat_info in data.features
            }
            features = []
            for feat_id in feature_list.feature_ids:
                feat_name = feature_id_to_name_map[feat_id]
                if feat_name in specified_feature_map:
                    version = specified_feature_map.pop(feat_name)
                    feature = await self.feature_service.get_document_by_name_and_version(
                        name=feat_name, version=version
                    )
                    features.append(feature)
                else:
                    # for semi-auto mode, use default feature id for non-specified features
                    # for manual mode, use the original feature id of the feature list for non-specified features
                    if data.mode == FeatureListNewVersionMode.SEMI_AUTO:
                        feat_id = feat_name_to_default_id_map[feat_name]
                    features.append(await self.feature_service.get_document(document_id=feat_id))

            if specified_feature_map:
                names = [f'"{name}"' for name in specified_feature_map.keys()]
                raise DocumentError(
                    f"Features ({', '.join(names)}) are not in the original FeatureList"
                )

        feature_ids = [feat.id for feat in features]
        if set(feature_list.feature_ids) == set(feature_ids):
            raise DocumentError("No change detected on the new feature list version.")

        return FeatureListModel(
            **{
                **feature_list.dict(),
                "_id": ObjectId(),
                "feature_ids": feature_ids,
                "features": features,
            }
        )

    async def create_new_feature_list_version(
        self,
        data: FeatureListNewVersionCreate,
    ) -> FeatureListModel:
        """
        Create new feature list version based on given source feature list & new version mode

        Parameters
        ----------
        data: FeatureListNewVersionCreate
            Version creation payload

        Returns
        -------
        FeatureListModel
        """
        feature_list = await self.feature_list_service.get_document(
            document_id=data.source_feature_list_id
        )
        feature_list_namespace = await self.feature_list_namespace_service.get_document(
            document_id=feature_list.feature_list_namespace_id,
        )
        feature_namespaces = await self.feature_namespace_service.list_documents(
            query_filter={"_id": {"$in": feature_list_namespace.feature_namespace_ids}},
            page_size=0,
        )
        new_feature_list = await self._create_new_feature_list_version(
            feature_list, feature_namespaces["data"], data
        )
        return await self.feature_list_service.create_document(
            data=FeatureListCreate(**new_feature_list.dict(by_alias=True)),
        )
