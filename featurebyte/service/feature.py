"""
FeatureService class
"""
from __future__ import annotations

from typing import Any

from bson.objectid import ObjectId

from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.enum import SourceType
from featurebyte.exception import (
    DocumentConflictError,
    DocumentNotFoundError,
    DuplicatedRegistryError,
)
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.feature_manager.snowflake_feature import FeatureManagerSnowflake
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.event_data import EventDataModel
from featurebyte.models.feature import DefaultVersionMode, FeatureModel, FeatureReadiness
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.schema.feature import FeatureCreate, FeatureInfo
from featurebyte.schema.feature_namespace import FeatureNamespaceCreate, FeatureNamespaceUpdate
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
        DocumentConflictError
            When the feature registry already exists at the feature store
        Exception
            Other errors during registry insertion / removal
        """
        feature_store = document.feature_store
        if feature_store.type == SourceType.SNOWFLAKE:
            db_session = feature_store.get_session(
                credentials={
                    feature_store.name: await get_credential(
                        user_id=self.user.id, feature_store_name=feature_store.name
                    )
                }
            )
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
                feature_namespace = await feature_namespace_service.get_document(
                    document_id=document.feature_namespace_id
                )

                # update feature namespace
                feature_namespace = await feature_namespace_service.update_document(
                    document_id=feature_namespace.id,
                    data=FeatureNamespaceUpdate(feature_id=document.id),
                )

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

    async def update_document(
        self, document_id: ObjectId, data: FeatureByteBaseModel
    ) -> FeatureModel:
        # TODO: implement proper logic to update feature document
        # when update the feature readiness, needs to update feature list's feature readiness distribution
        # and feature list namespace's feature readiness distribution
        return await self.get_document(document_id=document_id)

    async def get_info(
        self, document_id: ObjectId, page: int, page_size: int, verbose: bool
    ) -> FeatureInfo:
        feature = await self.get_document(document_id=document_id)
        feature_namespace_service = FeatureNamespaceService(
            user=self.user, persistent=self.persistent
        )
        namespace_info = await feature_namespace_service.get_info(
            document_id=feature.feature_namespace_id,
            page=page,
            page_size=page_size,
            verbose=verbose,
        )
        default_feature = await self.get_document(document_id=namespace_info.default_feature_id)
        return FeatureInfo(
            **namespace_info.dict(),
            version={"this": feature.version, "default": default_feature.version},
            readiness={"this": feature.readiness, "default": default_feature.readiness},
        )
