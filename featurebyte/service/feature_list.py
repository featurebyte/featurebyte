"""
FeatureListService class
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from bson.objectid import ObjectId

from featurebyte.exception import (
    DocumentError,
    DocumentInconsistencyError,
    DocumentNotFoundError,
)
from featurebyte.models.feature import DefaultVersionMode, FeatureModel, FeatureSignature
from featurebyte.models.feature_list import FeatureListModel, FeatureListNamespaceModel
from featurebyte.schema.feature import FeatureServiceUpdate
from featurebyte.schema.feature_list import (
    FeatureListBriefInfoList,
    FeatureListCreate,
    FeatureListInfo,
    FeatureListServiceUpdate,
)
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceServiceUpdate
from featurebyte.service.base_document import BaseDocumentService, GetInfoServiceMixin
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService


class FeatureListService(
    BaseDocumentService[FeatureListModel], GetInfoServiceMixin[FeatureListInfo]
):
    """
    FeatureListService class
    """

    document_class = FeatureListModel

    async def _extract_feature_data(self, document: FeatureListModel) -> Dict[str, Any]:
        feature_store_id: Optional[ObjectId] = None
        feature_signatures: List[FeatureSignature] = []
        feature_namespace_ids = set()
        features = []
        feature_service = FeatureService(user=self.user, persistent=self.persistent)
        for feature_id in document.feature_ids:
            # retrieve feature from the persistent
            feature = await feature_service.get_document(document_id=feature_id)

            # compute data required to create feature list record
            features.append(feature)
            feature_signatures.append(
                FeatureSignature(id=feature.id, name=feature.name, version=feature.version)
            )

            # validate the feature list
            if feature_store_id and (feature_store_id != feature.tabular_source.feature_store_id):
                raise DocumentInconsistencyError(
                    "All the Feature objects within the same FeatureList object must be from the same "
                    "feature store."
                )

            # check whether there are duplicated feature names in a feature list
            if feature.feature_namespace_id in feature_namespace_ids:
                raise DocumentError(
                    "Two Feature objects must not share the same name in a FeatureList object."
                )

            # update feature_namespace_ids
            feature_namespace_ids.add(feature.feature_namespace_id)

            # store previous feature store id
            feature_store_id = feature.tabular_source.feature_store_id

        derived_output = {
            "feature_store_id": feature_store_id,
            "feature_signatures": feature_signatures,
            "features": features,
        }
        return derived_output

    async def _update_features(
        self, features: list[FeatureModel], feature_list_id: ObjectId
    ) -> None:
        feature_service = FeatureService(user=self.user, persistent=self.persistent)
        for feature in features:
            await feature_service.update_document(
                document_id=feature.id,
                data=FeatureServiceUpdate(feature_list_id=feature_list_id),
                document=feature,
                return_document=False,
            )

    async def create_document(  # type: ignore[override]
        self, data: FeatureListCreate, get_credential: Any = None
    ) -> FeatureListModel:
        # sort feature_ids before saving to persistent storage to ease feature_ids comparison in uniqueness check
        document = FeatureListModel(
            **{
                **data.json_dict(),
                "feature_ids": sorted(data.feature_ids),
                "user_id": self.user.id,
            }
        )

        async with self.persistent.start_transaction() as session:
            # check any conflict with existing documents
            await self._check_document_unique_constraints(document=document)

            # check whether the feature(s) in the feature list saved to persistent or not
            feature_data = await self._extract_feature_data(document)

            # update document with derived output
            document = FeatureListModel(
                **document.dict(by_alias=True), features=feature_data["features"]
            )

            insert_id = await session.insert_one(
                collection_name=self.collection_name,
                document=document.dict(by_alias=True),
                user_id=self.user.id,
            )
            assert insert_id == document.id

            feature_list_namespace_service = FeatureListNamespaceService(
                user=self.user, persistent=self.persistent
            )
            try:
                # update feature list namespace
                feature_list_namespace = await feature_list_namespace_service.update_document(
                    document_id=document.feature_list_namespace_id,
                    data=FeatureListNamespaceServiceUpdate(feature_list_id=document.id),
                )
                assert feature_list_namespace is not None

            except DocumentNotFoundError:
                await feature_list_namespace_service.create_document(
                    data=FeatureListNamespaceModel(
                        _id=document.feature_list_namespace_id or ObjectId(),
                        name=document.name,
                        feature_list_ids=[insert_id],
                        readiness_distribution=document.readiness_distribution,
                        default_feature_list_id=insert_id,
                        default_version_mode=DefaultVersionMode.AUTO,
                        features=feature_data["features"],
                    )
                )

            # update feature's feature_list_ids attribute
            await self._update_features(feature_data["features"], insert_id)
        return await self.get_document(document_id=insert_id)

    async def update_document(  # type: ignore[override]
        self,
        document_id: ObjectId,
        data: FeatureListServiceUpdate,
        document: Optional[FeatureListModel] = None,
        return_document: bool = True,
    ) -> Optional[FeatureListModel]:
        if document is None:
            document = await self.get_document(document_id=document_id)

        if data.readiness_transition:
            # update feature list readiness distribution
            readiness_dist = document.readiness_distribution.update_readiness(
                transition=data.readiness_transition
            )
            _ = await self.persistent.update_one(
                collection_name=self.collection_name,
                query_filter={"_id": document.id},
                update={"$set": readiness_dist.dict()["__root__"]},
            )

            # trigger feature list namespace to check whether to update default feature list
            feature_list_namespace_service = FeatureListNamespaceService(
                user=self.user, persistent=self.persistent
            )
            _ = await feature_list_namespace_service.update_document(
                document_id=document.feature_list_namespace_id,
                data=FeatureListNamespaceServiceUpdate(feature_list_id=document_id),
            )

        if return_document:
            return document
        return None

    async def get_info(self, document_id: ObjectId, verbose: bool) -> FeatureListInfo:
        feature_list = await self.get_document(document_id=document_id)
        feature_list_namespace_service = FeatureListNamespaceService(
            user=self.user, persistent=self.persistent
        )
        namespace_info = await feature_list_namespace_service.get_info(
            document_id=feature_list.feature_list_namespace_id,
            verbose=verbose,
        )
        default_feature_list = await self.get_document(
            document_id=namespace_info.default_feature_list_id
        )
        versions_info = None
        if verbose:
            namespace = await feature_list_namespace_service.get_document(
                document_id=feature_list.feature_list_namespace_id
            )
            versions_info = FeatureListBriefInfoList.from_paginated_data(
                await self.list_documents(
                    page=1,
                    page_size=0,
                    query_filter={"_id": {"$in": namespace.feature_list_ids}},
                )
            )

        return FeatureListInfo(
            **namespace_info.dict(),
            version={"this": feature_list.version, "default": default_feature_list.version},
            production_ready_fraction={
                "this": feature_list.readiness_distribution.derive_production_ready_fraction(),
                "default": default_feature_list.readiness_distribution.derive_production_ready_fraction(),
            },
            versions_info=versions_info,
        )
