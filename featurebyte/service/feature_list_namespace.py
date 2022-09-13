"""
FeatureListNamespaceService class
"""
from __future__ import annotations

from typing import Any

from bson.objectid import ObjectId

from featurebyte.models.feature import DefaultVersionMode
from featurebyte.models.feature_list import (
    FeatureListModel,
    FeatureListNamespaceModel,
    FeatureReadinessDistribution,
)
from featurebyte.schema.entity import EntityBriefInfoList
from featurebyte.schema.event_data import EventDataBriefInfoList
from featurebyte.schema.feature_list_namespace import (
    FeatureListNamespaceInfo,
    FeatureListNamespaceUpdate,
)
from featurebyte.service.base_document import BaseDocumentService, GetInfoServiceMixin
from featurebyte.service.entity import EntityService
from featurebyte.service.event_data import EventDataService


class FeatureListNamespaceService(
    BaseDocumentService[FeatureListNamespaceModel], GetInfoServiceMixin[FeatureListNamespaceInfo]
):
    """
    FeatureListNamespaceService class
    """

    document_class = FeatureListNamespaceModel

    async def create_document(  # type: ignore[override]
        self, data: FeatureListNamespaceModel, get_credential: Any = None
    ) -> FeatureListNamespaceModel:
        _ = get_credential
        document = FeatureListNamespaceModel(**{**data.json_dict(), "user_id": self.user.id})
        # check any conflict with existing documents
        await self._check_document_unique_constraints(document=document)
        insert_id = await self.persistent.insert_one(
            collection_name=self.collection_name,
            document=document.dict(by_alias=True),
            user_id=self.user.id,
        )
        assert insert_id == data.id
        return await self.get_document(document_id=insert_id)

    async def update_document(  # type: ignore[override]
        self, document_id: ObjectId, data: FeatureListNamespaceUpdate
    ) -> FeatureListNamespaceModel:
        document = await self.get_document(
            document_id=document_id,
            exception_detail=f'FeatureListNamespace (id: "{document_id}") not found.',
        )

        feature_list_ids = list(document.feature_list_ids)
        default_feature_list_id = document.default_feature_list_id
        default_version_mode = DefaultVersionMode(document.default_version_mode)
        readiness_dist = document.readiness_distribution

        if data.default_version_mode:
            default_version_mode = DefaultVersionMode(data.default_version_mode)

        if data.feature_list_id:
            # check whether the feature list is saved to persistent or not
            feature_list_version_dict = await self._get_document(
                document_id=data.feature_list_id,
                collection_name=FeatureListModel.collection_name(),
            )

            version_readiness_dist = FeatureReadinessDistribution(
                __root__=feature_list_version_dict["readiness_distribution"]
            )
            # TODO: update the logic here when the feature_list_id is already in the feature list namespace
            feature_list_ids.append(feature_list_version_dict["_id"])
            readiness_dist = max(readiness_dist, version_readiness_dist)
            if (
                document.default_version_mode == DefaultVersionMode.AUTO
                and version_readiness_dist >= document.readiness_distribution  # type: ignore[operator]
            ):
                # if default version mode is AUTO, use the latest best readiness feature list as default feature
                default_feature_list_id = feature_list_version_dict["_id"]

        update_count = await self.persistent.update_one(
            collection_name=self.collection_name,
            query_filter={"_id": document.id},
            update={
                "$set": {
                    "feature_list_ids": feature_list_ids,
                    "readiness_distribution": readiness_dist.dict()["__root__"],
                    "default_feature_list_id": default_feature_list_id,
                    "default_version_mode": default_version_mode.value,
                }
            },
        )
        assert update_count == 1
        return await self.get_document(document_id=document_id)

    async def get_info(self, document_id: ObjectId, verbose: bool) -> FeatureListNamespaceInfo:
        namespace = await self.get_document(document_id=document_id)
        entity_service = EntityService(user=self.user, persistent=self.persistent)
        entities = await entity_service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": namespace.entity_ids}}
        )

        event_data_service = EventDataService(user=self.user, persistent=self.persistent)
        event_data = await event_data_service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": namespace.event_data_ids}}
        )
        return FeatureListNamespaceInfo(
            name=namespace.name,
            created_at=namespace.created_at,
            updated_at=namespace.updated_at,
            entities=EntityBriefInfoList.from_paginated_data(entities),
            event_data=EventDataBriefInfoList.from_paginated_data(event_data),
            default_version_mode=namespace.default_version_mode,
            default_feature_list_id=namespace.default_feature_list_id,
            dtype_distribution=namespace.dtype_distribution,
            version_count=len(namespace.feature_list_ids),
            feature_count=len(namespace.feature_namespace_ids),
            status=namespace.status,
        )
