"""
FeatureNamespaceService class
"""
from __future__ import annotations

from typing import Any, Optional

from bson.objectid import ObjectId

from featurebyte.models.feature import FeatureNamespaceModel
from featurebyte.schema.entity import EntityBriefInfoList
from featurebyte.schema.event_data import EventDataBriefInfoList
from featurebyte.schema.feature_namespace import (
    FeatureNamespaceCreate,
    FeatureNamespaceInfo,
    FeatureNamespaceServiceUpdate,
)
from featurebyte.service.base_document import BaseDocumentService, GetInfoServiceMixin
from featurebyte.service.entity import EntityService
from featurebyte.service.event_data import EventDataService


class FeatureNamespaceService(
    BaseDocumentService[FeatureNamespaceModel], GetInfoServiceMixin[FeatureNamespaceInfo]
):
    """
    FeatureNamespaceService class
    """

    document_class = FeatureNamespaceModel

    async def create_document(  # type: ignore[override]
        self, data: FeatureNamespaceCreate, get_credential: Any = None
    ) -> FeatureNamespaceModel:
        _ = get_credential
        document = FeatureNamespaceModel(**data.json_dict(), user_id=self.user.id)
        # check any conflict with existing documents
        await self._check_document_unique_constraints(document=document)
        insert_id = await self.persistent.insert_one(
            collection_name=self.collection_name,
            document=document.dict(by_alias=True),
            user_id=self.user.id,
        )
        assert insert_id == document.id
        return await self.get_document(document_id=insert_id)

    async def update_document(  # type: ignore[override]
        self,
        document_id: ObjectId,
        data: FeatureNamespaceServiceUpdate,
        exclude_none: bool = True,
        document: Optional[FeatureNamespaceModel] = None,
        return_document: bool = True,
    ) -> Optional[FeatureNamespaceModel]:
        if document is None:
            exception_detail = f'FeatureNamespace (id: "{document_id}") not found.'
            await self.get_document(document_id=document_id, exception_detail=exception_detail)

        await self.persistent.update_one(
            collection_name=self.collection_name,
            query_filter=self._construct_get_query_filter(document_id=document_id),
            update={"$set": data.dict(exclude_none=exclude_none)},
            user_id=self.user.id,
        )

        if return_document:
            return await self.get_document(document_id=document_id)
        return None

    async def get_info(self, document_id: ObjectId, verbose: bool) -> FeatureNamespaceInfo:
        namespace = await self.get_document(document_id=document_id)
        entity_service = EntityService(user=self.user, persistent=self.persistent)
        entities = await entity_service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": namespace.entity_ids}}
        )

        event_data_service = EventDataService(user=self.user, persistent=self.persistent)
        event_data = await event_data_service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": namespace.event_data_ids}}
        )
        return FeatureNamespaceInfo(
            name=namespace.name,
            created_at=namespace.created_at,
            updated_at=namespace.updated_at,
            entities=EntityBriefInfoList.from_paginated_data(entities),
            event_data=EventDataBriefInfoList.from_paginated_data(event_data),
            default_version_mode=namespace.default_version_mode,
            default_feature_id=namespace.default_feature_id,
            dtype=namespace.dtype,
            version_count=len(namespace.feature_ids),
        )
