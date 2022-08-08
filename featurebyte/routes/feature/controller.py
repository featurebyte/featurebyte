"""
Feature API route controller
"""
from __future__ import annotations

from typing import Any

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.enum import SourceType
from featurebyte.exception import DuplicatedRegistryError
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.feature_manager.snowflake_feature import FeatureManagerSnowflake
from featurebyte.models.base import UniqueConstraintResolutionSignature
from featurebyte.models.event_data import EventDataModel
from featurebyte.models.feature import (
    DefaultVersionMode,
    FeatureModel,
    FeatureNameSpaceModel,
    FeatureReadiness,
)
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.persistent import Persistent
from featurebyte.routes.common.base import BaseController
from featurebyte.schema.feature import FeatureCreate, FeatureList


class FeatureController(BaseController[FeatureModel, FeatureList]):
    """
    Feature controller
    """

    collection_name = FeatureModel.collection_name()
    document_class = FeatureModel
    paginated_document_class = FeatureList

    @classmethod
    async def _validate_feature(cls, user: Any, data: FeatureCreate, session: Persistent) -> None:
        """
        Validate feature document to make sure the feature & parent feature are valid

        Parameters
        ----------
        user: Any
            User object
        data: FeatureCreate
            Feature document
        session: Persistent
            Persistent session

        Raises
        ------
        HTTPException
            If the document failed validation checks
        """
        if data.parent_id is None:
            # when the parent_id is missing, it implies that the feature is a new feature
            await cls.check_document_unique_constraint(
                persistent=session,
                query_filter={"name": data.name},
                conflict_signature={"name": data.name},
                user_id=user.id,
                resolution_signature=UniqueConstraintResolutionSignature.GET_BY_ID,
            )
        else:
            # if parent_id exists, make sure the parent feature exists at persistent & has consistent name
            parent_feature_dict = await cls.get_document(
                user=user,
                persistent=session,
                collection_name=cls.collection_name,
                document_id=data.parent_id,
                exception_status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                exception_detail=(
                    f'The original feature (id: "{data.parent_id}") not found. '
                    "Please save the Feature object first."
                ),
            )
            parent_name = parent_feature_dict["name"]
            if parent_name != data.name:
                # if the parent feature is inconsistent with feature to be created, throws exception
                raise HTTPException(
                    status_code=HTTPStatus.CONFLICT,
                    detail=(
                        f'Feature (id: "{data.id}", name: "{data.name}") '
                        f'has invalid parent feature (id: "{data.parent_id}", name: "{parent_name}")!'
                    ),
                )

        for event_data_id in data.event_data_ids:
            _ = await cls.get_document(
                user=user,
                persistent=session,
                collection_name=EventDataModel.collection_name(),
                document_id=event_data_id,
                exception_status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            )

    @classmethod
    def prepare_feature_namespace_payload(
        cls, document: FeatureModel, feature_namespace: FeatureNameSpaceModel
    ) -> dict[str, Any]:
        """
        Prepare payload to update feature namespace record

        Parameters
        ----------
        document: FeatureModel
            Feature document
        feature_namespace: FeatureNameSpaceModel
            Feature Namespace object

        Returns
        -------
        dict
            Payload used to update feature namespace record
        """
        version_ids = feature_namespace.version_ids + [document.id]
        matched_versions = [
            ver for ver in feature_namespace.versions if ver.startswith(document.version)
        ]
        doc_version = document.version
        if matched_versions:
            doc_version = f"{doc_version}_{len(matched_versions)}"
        feature_versions = feature_namespace.versions + [doc_version]
        namespace_readiness = feature_namespace.readiness
        default_version_id: ObjectId = feature_namespace.default_version_id
        if feature_namespace.default_version_mode == DefaultVersionMode.AUTO:
            # this works only for the feature creation only where the readiness is DRAFT
            namespace_readiness = max(namespace_readiness, FeatureReadiness.DRAFT)
            if namespace_readiness == FeatureReadiness.DRAFT:
                default_version_id = document.id
        return {
            "versions": feature_versions,
            "version_ids": version_ids,
            "readiness": namespace_readiness,
            "default_version_id": default_version_id,
        }

    @classmethod
    async def _insert_feature_registry(
        cls,
        user: Any,
        document: FeatureModel,
        feature_store: ExtendedFeatureStoreModel,
        get_credential: Any,
    ) -> None:
        """
        Insert feature registry into feature store

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        document: FeatureModel
            Feature document
        feature_store: ExtendedFeatureStoreModel
            FeatureStore document
        get_credential: Any
            Get credential handler function

        Raises
        ------
        HTTPException
            When the feature registry already exists at the feature store
        Exception
            Other errors during registry insertion / removal
        """
        extended_feature = ExtendedFeatureModel(
            **document.dict(by_alias=True), feature_store=feature_store
        )
        if extended_feature.feature_store.type == SourceType.SNOWFLAKE:
            db_session = feature_store.get_session(
                credentials={
                    feature_store.name: await get_credential(
                        user_id=user.id, feature_store_name=feature_store.name
                    )
                }
            )
            feature_manager = FeatureManagerSnowflake(session=db_session)
            try:
                feature_manager.insert_feature_registry(extended_feature)
            except DuplicatedRegistryError as exc:
                # someone else already registered the feature at snowflake
                # do not remove the current registry & raise error to remove persistent record
                raise HTTPException(
                    status_code=HTTPStatus.CONFLICT,
                    detail=(
                        f'Feature (name: "{document.name}") has been registered by '
                        f"other feature at Snowflake feature store."
                    ),
                ) from exc
            except Exception as exc:
                # for other exceptions, cleanup feature registry record & persistent record
                feature_manager.remove_feature_registry(extended_feature)
                raise exc

    @classmethod
    async def create_feature(
        cls, user: Any, persistent: Persistent, get_credential: Any, data: FeatureCreate
    ) -> FeatureModel:
        """
        Create Feature at persistent (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that feature will be saved to
        get_credential: Any
            Get credential handler function
        data: FeatureCreate
            Feature creation payload

        Returns
        -------
        FeatureModel
            Newly created feature object
        """

        async with persistent.start_transaction() as session:
            # check any conflict with existing documents
            await cls.check_document_unique_constraint(
                persistent=session,
                query_filter={"_id": data.id},
                conflict_signature={"id": data.id},
                user_id=user.id,
                resolution_signature=UniqueConstraintResolutionSignature.GET_BY_ID,
            )

            document = FeatureModel(
                user_id=user.id, readiness=FeatureReadiness.DRAFT, **data.json_dict()
            )
            assert document.id == data.id

            # validate feature payload
            await cls._validate_feature(user=user, data=data, session=session)

            # get the feature store at persistent
            feature_store_id, _ = data.tabular_source
            feature_store_dict = await cls.get_document(
                user=user,
                persistent=session,
                collection_name=FeatureStoreModel.collection_name(),
                document_id=feature_store_id,
                exception_status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            )
            feature_store = ExtendedFeatureStoreModel(**feature_store_dict)

            insert_id = await session.insert_one(
                collection_name=cls.collection_name,
                document=document.dict(by_alias=True),
                user_id=user.id,
            )
            assert insert_id == document.id

            if document.parent_id is None:
                # create a new feature namespace object
                doc_feature_namespace = FeatureNameSpaceModel(
                    name=document.name,
                    version_ids=[insert_id],
                    versions=[document.version],
                    readiness=FeatureReadiness.DRAFT,
                    default_version_id=insert_id,
                    default_version_mode=DefaultVersionMode.AUTO,
                )
                await session.insert_one(
                    collection_name=FeatureNameSpaceModel.collection_name(),
                    document=doc_feature_namespace.dict(by_alias=True),
                    user_id=user.id,
                )
            else:
                # update feature namespace object
                feature_namespace_dict = await session.find_one(
                    collection_name=FeatureNameSpaceModel.collection_name(),
                    query_filter={"name": document.name},
                    user_id=user.id,
                )
                feature_namespace = FeatureNameSpaceModel(**feature_namespace_dict)  # type: ignore
                await session.update_one(
                    collection_name=FeatureNameSpaceModel.collection_name(),
                    query_filter={"_id": feature_namespace.id},
                    update={
                        "$set": cls.prepare_feature_namespace_payload(
                            document=document, feature_namespace=feature_namespace
                        )
                    },
                    user_id=user.id,
                )

            # insert feature registry into feature store
            await cls._insert_feature_registry(user, document, feature_store, get_credential)

        return await cls.get(user=user, persistent=persistent, document_id=insert_id)
