"""
FeatureListNamespaceService class
"""
from __future__ import annotations

from typing import Any

from bson.objectid import ObjectId

from featurebyte.models.feature import DefaultVersionMode, FeatureReadiness
from featurebyte.models.feature_list import FeatureListModel, FeatureListNamespaceModel
from featurebyte.schema.feature_list_namespace import (
    FeatureListNamespaceCreate,
    FeatureListNamespaceUpdate,
)
from featurebyte.service.base_document import BaseDocumentService


class FeatureListNamespaceService(BaseDocumentService[FeatureListNamespaceModel]):
    """
    FeatureListNamespaceService class
    """

    document_class = FeatureListNamespaceModel

    async def create_document(  # type: ignore[override]
        self, data: FeatureListNamespaceCreate, get_credential: Any = None
    ) -> FeatureListNamespaceModel:
        _ = get_credential
        document = FeatureListNamespaceModel(**data.json_dict(), user_id=self.user.id)
        insert_id = await self.persistent.insert_one(
            collection_name=self.collection_name,
            document=document.dict(by_alias=True),
            user_id=self.user.id,
        )
        assert insert_id == document.id
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
        readiness = FeatureReadiness(document.readiness)
        default_version_mode = DefaultVersionMode(document.default_version_mode)

        if data.default_version_mode:
            default_version_mode = DefaultVersionMode(data.default_version_mode)

        if data.feature_list_id:
            # check whether the feature list is saved to persistent or not
            feature_list_version_dict = await self._get_document(
                document_id=data.feature_list_id,
                collection_name=FeatureListModel.collection_name(),
            )

            feature_list_ids.append(feature_list_version_dict["_id"])
            readiness = max(readiness, FeatureReadiness(feature_list_version_dict["readiness"]))
            if (
                document.default_version_mode == DefaultVersionMode.AUTO
                and feature_list_version_dict["readiness"] >= document.readiness
            ):
                # if default version mode is AUTO, use the latest best readiness feature as default feature
                default_feature_list_id = feature_list_version_dict["_id"]

        update_count = await self.persistent.update_one(
            collection_name=self.collection_name,
            query_filter={"_id": document.id},
            update={
                "$set": {
                    "feature_list_ids": feature_list_ids,
                    "readiness": readiness.value,
                    "default_feature_list_id": default_feature_list_id,
                    "default_version_mode": default_version_mode.value,
                }
            },
        )
        assert update_count == 1
        return await self.get_document(document_id=document_id)
