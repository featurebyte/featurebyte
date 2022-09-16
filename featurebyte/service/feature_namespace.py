"""
FeatureNamespaceService class
"""
from __future__ import annotations

from typing import Any, Optional

from bson.objectid import ObjectId

from featurebyte.exception import DocumentInconsistencyError
from featurebyte.models.feature import (
    DefaultVersionMode,
    FeatureModel,
    FeatureNamespaceModel,
    FeatureReadiness,
)
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

    @staticmethod
    def _validate_feature_version_and_namespace_consistency(
        feature: FeatureModel, feature_namespace: FeatureNamespaceModel
    ) -> None:
        attrs = ["name", "dtype", "entity_ids", "event_data_ids"]
        for attr in attrs:
            version_attr = getattr(feature, attr)
            namespace_attr = getattr(feature_namespace, attr)
            version_attr_str: str | list[str] = f'"{version_attr}"'
            namespace_attr_str: str | list[str] = f'"{namespace_attr}"'
            if isinstance(version_attr, list):
                version_attr = sorted(version_attr)
                version_attr_str = [str(val) for val in version_attr]

            if isinstance(namespace_attr, list):
                namespace_attr = sorted(namespace_attr)
                namespace_attr_str = [str(val) for val in namespace_attr]

            if version_attr != namespace_attr:
                raise DocumentInconsistencyError(
                    f'Feature (name: "{feature.name}") object(s) within the same namespace '
                    f'must have the same "{attr}" value (namespace: {namespace_attr_str}, '
                    f"feature: {version_attr_str})."
                )

    @staticmethod
    def _prepare_default_version_mode_update_dict(
        update_data: FeatureNamespaceServiceUpdate, namespace: FeatureNamespaceModel
    ) -> dict[str, Any]:
        update_dict: dict[str, Any] = {}
        if (
            update_data.default_version_mode
            and update_data.default_version_mode != namespace.default_version_mode
        ):
            update_dict["default_version_mode"] = update_data.default_version_mode
        return update_dict

    @staticmethod
    async def _find_default_feature(
        namespace: FeatureNamespaceModel, default_feature: FeatureModel, feature_service: Any
    ) -> FeatureModel:
        readiness = min(FeatureReadiness)
        for feature_id in namespace.feature_ids:
            version = await feature_service.get_document(document_id=feature_id)
            assert version.created_at is not None
            if version.readiness > readiness:
                readiness = FeatureReadiness(version.readiness)
                default_feature = version
            elif version.readiness == readiness and version.created_at > default_feature.created_at:
                default_feature = version
        return default_feature

    async def _check_change_in_default_feature(
        self,
        update_data: FeatureNamespaceServiceUpdate,
        namespace: FeatureNamespaceModel,
        default_feature: FeatureModel,
        feature_service: Any,
    ) -> tuple[bool, dict[str, Any]]:
        update_dict: dict[str, Any] = {}
        default_version_mode = update_data.default_version_mode or namespace.default_version_mode
        to_find_default_feature = False
        if update_data.feature_id:
            # check whether the feature has been saved to persistent or not
            feature = await feature_service.get_document(document_id=update_data.feature_id)
            assert feature.created_at is not None
            self._validate_feature_version_and_namespace_consistency(
                feature=feature, feature_namespace=namespace
            )

            if feature.id not in namespace.feature_ids:
                # when a new feature version is added to the namespace
                update_dict["feature_ids"] = sorted(namespace.feature_ids + [feature.id])
                if default_version_mode == DefaultVersionMode.AUTO:
                    if (
                        FeatureReadiness(feature.readiness) >= namespace.readiness
                        and feature.created_at > default_feature.created_at
                    ):
                        update_dict["readiness"] = feature.readiness
                        update_dict["default_feature_id"] = feature.id
            elif default_version_mode == DefaultVersionMode.AUTO:
                to_find_default_feature = True
        elif default_version_mode == DefaultVersionMode.AUTO:
            to_find_default_feature = True
        return to_find_default_feature, update_dict

    async def _prepare_update_dict(
        self,
        update_data: FeatureNamespaceServiceUpdate,
        namespace: FeatureNamespaceModel,
    ) -> dict[str, Any]:
        from featurebyte.service.feature import (  # pylint: disable=import-outside-toplevel,cyclic-import
            FeatureService,
        )

        feature_service = FeatureService(user=self.user, persistent=self.persistent)
        default_feature = await feature_service.get_document(
            document_id=namespace.default_feature_id
        )
        assert default_feature.created_at is not None

        update_dict = self._prepare_default_version_mode_update_dict(
            update_data=update_data, namespace=namespace
        )
        (
            to_find_default_feature,
            default_feat_update_dict,
        ) = await self._check_change_in_default_feature(
            update_data, namespace, default_feature, feature_service
        )
        update_dict.update(default_feat_update_dict)

        if to_find_default_feature:
            default_feature = await self._find_default_feature(
                namespace, default_feature, feature_service
            )
            update_dict["readiness"] = default_feature.readiness
            update_dict["default_feature_id"] = default_feature.id
        return update_dict

    async def update_document(  # type: ignore[override]
        self,
        document_id: ObjectId,
        data: FeatureNamespaceServiceUpdate,
        document: Optional[FeatureNamespaceModel] = None,
        return_document: bool = True,
    ) -> Optional[FeatureNamespaceModel]:
        # pylint: disable=duplicate-code
        if document is None:
            document = await self.get_document(
                document_id=document_id,
                exception_detail=f'FeatureNamespace (id: "{document_id}") not found.',
            )

        update_dict = await self._prepare_update_dict(update_data=data, namespace=document)
        _ = await self.persistent.update_one(
            collection_name=self.collection_name,
            query_filter={"_id": document.id},
            update={"$set": update_dict},
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
