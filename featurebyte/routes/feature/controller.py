"""
Feature API route controller
"""
from __future__ import annotations

from typing import Any

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.enum import CollectionName, SourceType
from featurebyte.exception import DuplicatedFeatureRegistryError
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.feature_manager.snowflake_feature import FeatureManagerSnowflake
from featurebyte.models.feature import DefaultVersionMode, FeatureReadiness
from featurebyte.persistent import Persistent
from featurebyte.routes.common.util import get_utc_now
from featurebyte.schema.feature import Feature, FeatureCreate, FeatureNameSpace


class FeatureController:
    """
    Feature controller
    """

    collection_name = CollectionName.FEATURE

    @classmethod
    async def _validate_feature(cls, data: FeatureCreate, session: Persistent) -> None:
        """
        Validate feature create data to make sure the feature & parent feature are valid

        Parameters
        ----------
        data: FeatureCreate
            Feature creation data
        session: Persistent
            Persistent session

        Raises
        ------
        HTTPException
            If the data failed validation checks
        """
        if data.parent_id is None:
            # when the parent_id is missing, it implies that the feature is a new feature
            conflict_feature = await session.find_one(
                collection_name=cls.collection_name, query_filter={"name": data.name}
            )
            if conflict_feature:
                # if it is not a new feature throws exception
                raise HTTPException(
                    status_code=HTTPStatus.CONFLICT,
                    detail=f'Feature name (feature.name: "{data.name}") already exists.',
                )
        else:
            # if parent_id exists, make sure the parent feature exists at persistent & has consistent name
            parent_feature_dict = await session.find_one(
                collection_name=cls.collection_name,
                query_filter={"_id": ObjectId(data.parent_id)},
            )
            if not parent_feature_dict:
                # if parent feature not found at persistent, throws exception
                raise HTTPException(
                    status_code=HTTPStatus.NOT_FOUND,
                    detail=(
                        f'The original feature (feature.id: "{data.parent_id}") not found! '
                        f"Please save the Feature object first."
                    ),
                )
            if parent_feature_dict["name"] != data.name:
                # if the parent feature is inconsistent with feature to be created, throws exception
                raise HTTPException(
                    status_code=HTTPStatus.CONFLICT,
                    detail=(
                        f'Feature (feature.id: "{data.id}", feature.parent_id: "{data.parent_id}") '
                        f"has invalid parent feature!"
                    ),
                )

        for event_data_id in data.event_data_ids:
            event_data_dict = await session.find_one(
                collection_name=CollectionName.EVENT_DATA,
                query_filter={"_id": ObjectId(event_data_id)},
            )
            if not event_data_dict:
                # if event data not saved at persistent, throws exception
                raise HTTPException(
                    status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                    detail=(
                        f'EventData (event_data.id: "{event_data_id}") not found! '
                        f"Please save the EventData object first."
                    ),
                )

    @classmethod
    def prepare_feature_namespace_payload(
        cls, document: Feature, feature_namespace: FeatureNameSpace
    ) -> dict[str, Any]:
        """
        Prepare payload to update feature namespace record

        Parameters
        ----------
        document: Feature
            Feature document
        feature_namespace: FeatureNameSpace
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
    def insert_feature_registry(
        cls,
        user: Any,
        document: Feature,
        feature_store: ExtendedFeatureStoreModel,
        get_credential: Any,
    ) -> None:
        """
        Insert feature registry into feature store

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        document: Feature
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
                    feature_store.name: get_credential(
                        user_id=user.id, db_source_name=feature_store.name
                    )
                }
            )
            feature_manager = FeatureManagerSnowflake(session=db_session)
            try:
                feature_manager.insert_feature_registry(extended_feature)
            except DuplicatedFeatureRegistryError as exc:
                # someone else already registered the feature at snowflake
                # do not remove the current registry & raise error to remove persistent record
                raise HTTPException(
                    status_code=HTTPStatus.CONFLICT,
                    detail=(
                        f'Feature (feature.name: "{document.name}") has been registered by '
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
    ) -> Feature:
        """
        Create Feature at persistent (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that entity will be saved to
        get_credential: Any
            Get credential handler function
        data: FeatureCreate
            Feature creation payload

        Returns
        -------
        Feature
            Newly created feature object

        Raises
        ------
        HTTPException
            If the feature name conflicts with existing feature name
        """

        async with persistent.start_transaction() as session:
            if data.id:
                conflict_feature = await session.find_one(
                    collection_name=cls.collection_name, query_filter={"_id": data.id}
                )
                if conflict_feature:
                    raise HTTPException(
                        status_code=HTTPStatus.CONFLICT,
                        detail=f'Feature (feature.id: "{data.id}") has been saved before.',
                    )

            # validate feature payload
            await cls._validate_feature(data=data, session=session)

            utcnow = get_utc_now()
            document = Feature(
                user_id=user.id,
                created_at=utcnow,
                readiness=FeatureReadiness.DRAFT,
                **data.dict(by_alias=True),
            )
            assert document.id == data.id

            # get feature store
            feature_store_dict = await session.find_one(
                collection_name=CollectionName.FEATURE_STORE,
                query_filter={"_id": document.tabular_source[0]},
            )
            if not feature_store_dict:
                # if event data not saved at persistent, throws exception
                raise HTTPException(
                    status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                    detail=(
                        f'FeatureStore (feature_store.id: "{document.tabular_source[0]}") not found! '
                        f"Please save the FeatureStore object first."
                    ),
                )
            feature_store = ExtendedFeatureStoreModel(**feature_store_dict)

            insert_id = await session.insert_one(
                collection_name=cls.collection_name, document=document.dict(by_alias=True)
            )
            assert insert_id == document.id

            if document.parent_id is None:
                # create a new feature namespace object
                doc_feature_namespace = FeatureNameSpace(
                    name=document.name,
                    version_ids=[insert_id],
                    versions=[document.version],
                    readiness=FeatureReadiness.DRAFT,
                    created_at=utcnow,
                    default_version_id=insert_id,
                    default_version_mode=DefaultVersionMode.AUTO,
                )
                await session.insert_one(
                    collection_name=CollectionName.FEATURE_NAMESPACE,
                    document=doc_feature_namespace.dict(by_alias=True),
                )
            else:
                # update feature namespace object
                feature_namespace_dict = await session.find_one(
                    collection_name=CollectionName.FEATURE_NAMESPACE,
                    query_filter={"name": document.name},
                )
                feature_namespace = FeatureNameSpace(**feature_namespace_dict)  # type: ignore
                await session.update_one(
                    collection_name=CollectionName.FEATURE_NAMESPACE,
                    query_filter={"_id": feature_namespace.id},
                    update={
                        "$set": cls.prepare_feature_namespace_payload(
                            document=document, feature_namespace=feature_namespace
                        )
                    },
                )

            # insert feature registry into feature store
            cls.insert_feature_registry(user, document, feature_store, get_credential)

        return document
