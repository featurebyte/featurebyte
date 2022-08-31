"""
FeatureNamespaceService class
"""
from __future__ import annotations

from typing import Any

from bson.objectid import ObjectId

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.feature import (
    DefaultVersionMode,
    FeatureModel,
    FeatureNamespaceModel,
    FeatureReadiness,
)
from featurebyte.schema.feature_namespace import FeatureNamespaceCreate, FeatureNamespaceUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.common.operation import DictProject, DictTransform


class FeatureNamespaceService(BaseDocumentService[FeatureNamespaceModel]):
    """
    FeatureNamespaceService class
    """

    document_class = FeatureNamespaceModel
    info_transform = DictTransform(
        rule={
            **BaseDocumentService.base_info_transform_rule,
            "__root__": DictProject(rule=["default_version_mode", "default_version"]),
            "versions": DictProject(rule="version"),
        }
    )
    foreign_key_map = {
        "version_ids": FeatureModel.collection_name(),
        "default_version_id": FeatureModel.collection_name(),
    }

    async def create_document(
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

    async def update_document(
        self, document_id: ObjectId, data: FeatureNamespaceUpdate
    ) -> FeatureNamespaceModel:
        document = await self.get_document(
            document_id=document_id,
            exception_detail=f'FeatureNamespace (id: "{document_id}") not found.',
        )

        version_ids = list(document.version_ids)
        default_version_id = document.default_version_id
        readiness = FeatureReadiness(document.readiness)
        default_version_mode = DefaultVersionMode(document.default_version_mode)

        if data.default_version_mode:
            default_version_mode = DefaultVersionMode(data.default_version_mode)

        if data.version_id:
            # check whether the feature is saved to persistent or not
            feature = await self._get_document(
                document_id=data.version_id,
                collection_name=FeatureModel.collection_name(),
            )
            if feature["name"] != document.name:
                # sanity check that the feature namespace id has consistent name with feature name
                raise DocumentUpdateError(
                    f'Feature (name: "{feature["name"]}") has an inconsistent '
                    f'feature_namespace_id (name: "{document.name}").'
                )

            version_ids.append(feature["_id"])
            readiness = max(readiness, FeatureReadiness(feature["readiness"]))
            if (
                document.default_version_mode == DefaultVersionMode.AUTO
                and feature["readiness"] >= document.readiness
            ):
                # if default version mode is AUTO, use the latest best readiness feature as default feature
                default_version_id = feature["_id"]

        update_count = await self.persistent.update_one(
            collection_name=self.collection_name,
            query_filter={"_id": document.id},
            update={
                "$set": {
                    "version_ids": version_ids,
                    "readiness": readiness.value,
                    "default_version_id": default_version_id,
                    "default_version_mode": default_version_mode.value,
                }
            },
        )
        assert update_count == 1
        return await self.get_document(document_id=document_id)
