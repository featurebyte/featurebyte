"""
FeatureListNamespaceService class
"""
from __future__ import annotations

from typing import Any, Optional

from bson.objectid import ObjectId

from featurebyte.exception import DocumentInconsistencyError
from featurebyte.models.feature import DefaultVersionMode
from featurebyte.models.feature_list import FeatureListModel, FeatureListNamespaceModel
from featurebyte.schema.entity import EntityBriefInfoList
from featurebyte.schema.event_data import EventDataBriefInfoList
from featurebyte.schema.feature_list_namespace import (
    FeatureListNamespaceInfo,
    FeatureListNamespaceServiceUpdate,
)
from featurebyte.service.base_document import BaseDocumentService, GetInfoServiceMixin
from featurebyte.service.entity import EntityService
from featurebyte.service.event_data import EventDataService
from featurebyte.service.feature import FeatureService


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

    @staticmethod
    async def _validate_feature_list_version_and_namespace_consistency(
        feature_list: FeatureListModel,
        feature_list_namespace: FeatureListNamespaceModel,
        feature_service: FeatureService,
    ) -> None:
        feature_namespace_ids = []
        for feature_id in feature_list.feature_ids:
            feature = await feature_service.get_document(document_id=feature_id)
            feature_namespace_ids.append(feature.feature_namespace_id)

        if sorted(feature_namespace_ids) != sorted(feature_list_namespace.feature_namespace_ids):
            raise DocumentInconsistencyError(
                f'FeatureList (name: "{feature_list.name}") object(s) within the same namespace '
                f"must share the same feature name(s)."
            )

    @staticmethod
    def _prepare_default_version_mode_and_status_update_dict(
        update_data: FeatureListNamespaceServiceUpdate,
        namespace: FeatureListNamespaceModel,
    ) -> dict[str, Any]:
        update_dict: dict[str, Any] = {}
        if update_data.status and update_data.status != namespace.status:
            update_dict["status"] = update_data.status

        if (
            update_data.default_version_mode
            and update_data.default_version_mode != namespace.default_version_mode
        ):
            update_dict["default_version_mode"] = update_data.default_version_mode
        return update_dict

    @staticmethod
    async def _find_default_feature_list(
        namespace: FeatureListNamespaceModel,
        default_feature_list: FeatureListModel,
        feature_list_service: Any,
    ) -> FeatureListModel:
        readiness_dist = namespace.readiness_distribution.worst_case()
        for feature_list_id in namespace.feature_list_ids:
            version = await feature_list_service.get_document(document_id=feature_list_id)
            assert version.created_at is not None
            if version.readiness_distribution > readiness_dist:
                readiness_dist = version.readiness_distribution
                default_feature_list = version
            elif (
                version.readiness_distribution == readiness_dist
                and version.created_at > default_feature_list.created_at
            ):
                default_feature_list = version
        return default_feature_list

    async def _check_check_in_default_feature_list(
        self,
        update_data: FeatureListNamespaceServiceUpdate,
        namespace: FeatureListNamespaceModel,
        default_fl: FeatureListModel,
        feature_list_service: Any,
    ) -> tuple[bool, dict[str, Any]]:
        update_dict: dict[str, Any] = {}
        default_version_mode = update_data.default_version_mode or namespace.default_version_mode
        to_find_default_feature_list = False
        if update_data.feature_list_id:
            # check whether the feature list has been saved to persistent or not
            flist = await feature_list_service.get_document(document_id=update_data.feature_list_id)
            assert flist.created_at is not None
            await self._validate_feature_list_version_and_namespace_consistency(
                feature_list=flist,
                feature_list_namespace=namespace,
                feature_service=FeatureService(user=self.user, persistent=self.persistent),
            )

            if flist.id not in namespace.feature_list_ids:
                # when a new feature list version is added to the namespace
                update_dict["feature_list_ids"] = sorted(namespace.feature_list_ids + [flist.id])
                if default_version_mode == DefaultVersionMode.AUTO:
                    if (
                        flist.readiness_distribution >= namespace.readiness_distribution
                        and flist.created_at > default_fl.created_at
                    ):
                        read_dist_dict = flist.readiness_distribution.dict()["__root__"]
                        update_dict["readiness_distribution"] = read_dist_dict
                        update_dict["default_feature_list_id"] = flist.id
            elif default_version_mode == DefaultVersionMode.AUTO:
                to_find_default_feature_list = True
        elif default_version_mode == DefaultVersionMode.AUTO:
            to_find_default_feature_list = True
        return to_find_default_feature_list, update_dict

    async def _prepare_update_dict(
        self,
        update_data: FeatureListNamespaceServiceUpdate,
        namespace: FeatureListNamespaceModel,
    ) -> dict[str, Any]:
        from featurebyte.service.feature_list import (  # pylint: disable=import-outside-toplevel,cyclic-import
            FeatureListService,
        )

        feature_list_service = FeatureListService(user=self.user, persistent=self.persistent)
        default_fl = await feature_list_service.get_document(
            document_id=namespace.default_feature_list_id
        )
        assert default_fl.created_at is not None

        update_dict = self._prepare_default_version_mode_and_status_update_dict(
            update_data=update_data, namespace=namespace
        )
        (
            to_find_default_feature_list,
            default_fl_update_dict,
        ) = await self._check_check_in_default_feature_list(
            update_data, namespace, default_fl, feature_list_service
        )
        update_dict.update(default_fl_update_dict)

        if to_find_default_feature_list:
            default_fl = await self._find_default_feature_list(
                namespace, default_fl, feature_list_service
            )
            default_read_dist = default_fl.readiness_distribution.dict()["__root__"]
            update_dict["readiness_distribution"] = default_read_dist
            update_dict["default_feature_list_id"] = default_fl.id
        return update_dict

    async def update_document(  # type: ignore[override]
        self,
        document_id: ObjectId,
        data: FeatureListNamespaceServiceUpdate,
        document: Optional[FeatureListNamespaceModel] = None,
        return_document: bool = True,
    ) -> Optional[FeatureListNamespaceModel]:
        # pylint: disable=duplicate-code
        if document is None:
            document = await self.get_document(
                document_id=document_id,
                exception_detail=f'FeatureListNamespace (id: "{document_id}") not found.',
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
