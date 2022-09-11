"""
FeatureListService class
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from bson.objectid import ObjectId

from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.enum import SourceType
from featurebyte.exception import (
    DocumentConflictError,
    DocumentError,
    DocumentInconsistencyError,
    DocumentNotFoundError,
    DuplicatedRegistryError,
)
from featurebyte.feature_manager.model import ExtendedFeatureListModel
from featurebyte.feature_manager.snowflake_feature_list import FeatureListManagerSnowflake
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature import DefaultVersionMode, FeatureModel, FeatureSignature
from featurebyte.models.feature_list import FeatureListModel, FeatureListNamespaceModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.schema.feature_list import FeatureListCreate, FeatureListInfo
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceUpdate
from featurebyte.service.base_document import BaseDocumentService, GetInfoServiceMixin
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService


class FeatureListService(
    BaseDocumentService[FeatureListModel], GetInfoServiceMixin[FeatureListInfo]
):
    """
    FeatureListService class
    """

    document_class = FeatureListModel

    async def _insert_feature_list_registry(
        self,
        document: ExtendedFeatureListModel,
        feature_store: ExtendedFeatureStoreModel,
        get_credential: Any,
    ) -> None:
        """
        Insert feature list registry into feature list store

        Parameters
        ----------
        document: ExtendedFeatureListModel
            ExtendedFeatureList document
        feature_store: ExtendedFeatureStoreModel
            FeatureStore document
        get_credential: Any
            Get credential handler function

        Raises
        ------
        DocumentConflictError
            When the feature registry already exists at the feature store
        """
        if feature_store.type == SourceType.SNOWFLAKE:
            db_session = feature_store.get_session(
                credentials={
                    feature_store.name: await get_credential(
                        user_id=self.user.id, feature_store_name=feature_store.name
                    )
                }
            )
            feature_list_manager = FeatureListManagerSnowflake(session=db_session)
            try:
                feature_list_manager.insert_feature_list_registry(document)
            except DuplicatedRegistryError as exc:
                # someone else already registered the feature at snowflake
                # do not remove the current registry & raise error to remove persistent record
                raise DocumentConflictError(
                    f'FeatureList (name: "{document.name}") has been registered by '
                    f"other feature list at Snowflake feature list store."
                ) from exc

    async def _extract_feature_data(self, document: FeatureListModel) -> Dict[str, Any]:
        feature_store_id: Optional[ObjectId] = None
        feature_signatures: List[FeatureSignature] = []
        feature_namespace_ids = set()
        features = []
        for feature_id in document.feature_ids:
            # retrieve feature from the persistent
            feature_dict = await self._get_document(
                document_id=feature_id,
                collection_name=FeatureModel.collection_name(),
            )
            feature = FeatureModel(**feature_dict)

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

            feature_store_dict = await self._get_document(
                document_id=ObjectId(feature_data["feature_store_id"]),
                collection_name=FeatureStoreModel.collection_name(),
            )
            feature_store = ExtendedFeatureStoreModel(**feature_store_dict)

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
                feature_list_namespace = await feature_list_namespace_service.get_document(
                    document_id=document.feature_list_namespace_id
                )

                # update feature list namespace
                feature_list_namespace = await feature_list_namespace_service.update_document(
                    document_id=feature_list_namespace.id,
                    data=FeatureListNamespaceUpdate(feature_list_id=document.id),
                )

            except DocumentNotFoundError:
                feature_list_namespace = await feature_list_namespace_service.create_document(
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

            # insert feature list registry into feature list store
            await self._insert_feature_list_registry(
                document=ExtendedFeatureListModel(
                    **document.dict(by_alias=True),
                    feature_signatures=feature_data["feature_signatures"],
                    status=feature_list_namespace.status,
                ),
                feature_store=feature_store,
                get_credential=get_credential,
            )
        return await self.get_document(document_id=insert_id)

    async def update_document(
        self, document_id: ObjectId, data: FeatureByteBaseModel
    ) -> FeatureListModel:
        # TODO: implement proper logic to update feature list document
        return await self.get_document(document_id=document_id)

    async def get_info(self, document_id: ObjectId, page: int, page_size: int) -> FeatureListInfo:
        feature_list = await self.get_document(document_id=document_id)
        feature_list_namespace_service = FeatureListNamespaceService(
            user=self.user, persistent=self.persistent
        )
        namespace_info = await feature_list_namespace_service.get_info(
            document_id=feature_list.feature_list_namespace_id, page=page, page_size=page_size
        )
        default_feature_list = await self.get_document(
            document_id=namespace_info.default_feature_list_id
        )
        return FeatureListInfo(
            **namespace_info.dict(),
            version={"this": feature_list.version, "default": default_feature_list.version},
            production_ready_fraction={
                "this": feature_list.readiness_distribution.derive_production_ready_fraction(),
                "default": default_feature_list.readiness_distribution.derive_production_ready_fraction(),
            },
        )
