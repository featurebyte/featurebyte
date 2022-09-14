"""
FeatureService class
"""
from __future__ import annotations

from typing import Any, Optional

from bson.objectid import ObjectId
from pydantic import ValidationError

from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.enum import SourceType
from featurebyte.exception import (
    CredentialsError,
    DocumentConflictError,
    DocumentNotFoundError,
    DuplicatedRegistryError,
)
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.feature_manager.snowflake_feature import FeatureManagerSnowflake
from featurebyte.models.event_data import EventDataModel
from featurebyte.models.feature import DefaultVersionMode, FeatureModel, FeatureReadiness
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.schema.feature import (
    FeatureBriefInfoList,
    FeatureCreate,
    FeatureInfo,
    FeatureServiceUpdate,
)
from featurebyte.schema.feature_namespace import (
    FeatureNamespaceCreate,
    FeatureNamespaceServiceUpdate,
)
from featurebyte.service.base_document import BaseDocumentService, GetInfoServiceMixin
from featurebyte.service.feature_namespace import FeatureNamespaceService


class FeatureService(BaseDocumentService[FeatureModel], GetInfoServiceMixin[FeatureInfo]):
    """
    FeatureService class
    """

    document_class = FeatureModel

    async def _insert_feature_registry(
        self, document: ExtendedFeatureModel, get_credential: Any
    ) -> None:
        """
        Insert feature registry into feature store

        Parameters
        ----------
        document: ExtendedFeatureModel
            Feature document
        get_credential: Any
            Get credential handler function

        Raises
        ------
        CredentialsError
            When the credentials used to access the feature store is missing or invalid
        DocumentConflictError
            When the feature registry already exists at the feature store
        Exception
            Other errors during registry insertion / removal
        """
        feature_store = document.feature_store
        if feature_store.type == SourceType.SNOWFLAKE:
            try:
                db_session = feature_store.get_session(
                    credentials={
                        feature_store.name: await get_credential(
                            user_id=self.user.id, feature_store_name=feature_store.name
                        )
                    }
                )
            except ValidationError as exc:
                raise CredentialsError(
                    f'Credential used to access FeatureStore (name: "{feature_store.name}") is missing or invalid.'
                ) from exc

            feature_manager = FeatureManagerSnowflake(session=db_session)
            try:
                feature_manager.insert_feature_registry(document)
            except DuplicatedRegistryError as exc:
                # someone else already registered the feature at snowflake
                # do not remove the current registry & raise error to remove persistent record
                raise DocumentConflictError(
                    f'Feature (name: "{document.name}") has been registered by '
                    f"other feature at Snowflake feature store."
                ) from exc
            except Exception as exc:
                # for other exceptions, cleanup feature registry record & persistent record
                feature_manager.remove_feature_registry(document)
                raise exc

    async def create_document(  # type: ignore[override]
        self, data: FeatureCreate, get_credential: Any = None
    ) -> FeatureModel:
        document = FeatureModel(
            **{**data.json_dict(), "readiness": FeatureReadiness.DRAFT, "user_id": self.user.id}
        )

        async with self.persistent.start_transaction() as session:
            # check any conflict with existing documents
            await self._check_document_unique_constraints(document=document)

            # check event_data has been saved at persistent storage or not
            for event_data_id in data.event_data_ids:
                _ = await self._get_document(
                    document_id=event_data_id,
                    collection_name=EventDataModel.collection_name(),
                )

            insert_id = await session.insert_one(
                collection_name=self.collection_name,
                document=document.dict(by_alias=True),
                user_id=self.user.id,
            )
            assert insert_id == document.id

            feature_namespace_service = FeatureNamespaceService(
                user=self.user, persistent=self.persistent
            )
            try:
                # update feature namespace
                feature_namespace = await feature_namespace_service.update_document(
                    document_id=document.feature_namespace_id,
                    data=FeatureNamespaceServiceUpdate(feature_id=document.id),
                )
                assert feature_namespace is not None

            except DocumentNotFoundError:
                feature_namespace = await feature_namespace_service.create_document(
                    data=FeatureNamespaceCreate(
                        _id=document.feature_namespace_id,
                        name=document.name,
                        dtype=document.dtype,
                        feature_ids=[insert_id],
                        readiness=FeatureReadiness.DRAFT,
                        default_feature_id=insert_id,
                        default_version_mode=DefaultVersionMode.AUTO,
                        entity_ids=sorted(document.entity_ids),
                        event_data_ids=sorted(document.event_data_ids),
                    ),
                )

            # insert feature registry into feature store
            feature_store_dict = await self._get_document(
                document_id=data.tabular_source.feature_store_id,
                collection_name=FeatureStoreModel.collection_name(),
            )
            feature_store = ExtendedFeatureStoreModel(**feature_store_dict)
            extended_feature = ExtendedFeatureModel(
                **document.dict(by_alias=True),
                is_default=document.id == feature_namespace.default_feature_id,
                feature_store=feature_store,
            )
            await self._insert_feature_registry(extended_feature, get_credential)
        return await self.get_document(document_id=insert_id)

    async def update_document(  # type: ignore[override]
        self,
        document_id: ObjectId,
        data: FeatureServiceUpdate,
        document: Optional[FeatureModel] = None,
        return_document: bool = True,
    ) -> Optional[FeatureModel]:
        if document is None:
            document = await self.get_document(document_id=document_id)

        update_payload: dict[str, Any] = {}
        if data.readiness:
            update_payload["readiness"] = data.readiness.value
        if data.feature_list_id:
            feature_list_ids = set(document.feature_list_ids + [data.feature_list_id])
            update_payload["feature_list_ids"] = sorted(feature_list_ids)

        async with self.persistent.start_transaction() as session:
            if update_payload:
                await session.update_one(
                    collection_name=self.collection_name,
                    query_filter=self._construct_get_query_filter(document_id=document_id),
                    update={"$set": update_payload},
                    user_id=self.user.id,
                )

            if data.readiness:
                feature_namespace_service = FeatureNamespaceService(
                    user=self.user, persistent=self.persistent
                )
                await feature_namespace_service.update_document(
                    document_id=document.feature_namespace_id,
                    data=FeatureNamespaceServiceUpdate(
                        feature_id=document_id,
                    ),
                )

        if return_document:
            return await self.get_document(document_id=document_id)
        return None

    async def get_info(self, document_id: ObjectId, verbose: bool) -> FeatureInfo:
        feature = await self.get_document(document_id=document_id)
        feature_namespace_service = FeatureNamespaceService(
            user=self.user, persistent=self.persistent
        )
        namespace_info = await feature_namespace_service.get_info(
            document_id=feature.feature_namespace_id,
            verbose=verbose,
        )
        default_feature = await self.get_document(document_id=namespace_info.default_feature_id)
        versions_info = None
        if verbose:
            namespace = await feature_namespace_service.get_document(
                document_id=feature.feature_namespace_id
            )
            versions_info = FeatureBriefInfoList.from_paginated_data(
                await self.list_documents(
                    page=1,
                    page_size=0,
                    query_filter={"_id": {"$in": namespace.feature_ids}},
                )
            )

        return FeatureInfo(
            **namespace_info.dict(),
            version={"this": feature.version, "default": default_feature.version},
            readiness={"this": feature.readiness, "default": default_feature.readiness},
            versions_info=versions_info,
        )
