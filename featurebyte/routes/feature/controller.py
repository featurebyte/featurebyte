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
from featurebyte.models.event_data import EventDataModel
from featurebyte.models.feature import DefaultVersionMode, FeatureModel, FeatureReadiness
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.persistent import Persistent
from featurebyte.routes.common.base import BaseController
from featurebyte.routes.feature_namespace.controller import FeatureNamespaceController
from featurebyte.schema.feature import FeatureCreate, FeatureList
from featurebyte.schema.feature_namespace import FeatureNamespaceCreate, FeatureNamespaceUpdate


class FeatureController(BaseController[FeatureModel, FeatureList]):
    """
    Feature controller
    """

    collection_name = FeatureModel.collection_name()
    document_class = FeatureModel
    paginated_document_class = FeatureList

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
            document = FeatureModel(
                **{
                    **data.json_dict(),
                    "user_id": user.id,
                    "readiness": FeatureReadiness.DRAFT,
                }
            )
            assert document.id == data.id

            # check any conflict with existing documents
            await cls.check_document_unique_constraints(
                persistent=persistent,
                user_id=user.id,
                document=document,
            )

            # check event_data has been saved at persistent storage or not
            for event_data_id in data.event_data_ids:
                _ = await cls.get_document(
                    user=user,
                    persistent=session,
                    collection_name=EventDataModel.collection_name(),
                    document_id=event_data_id,
                    exception_status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                )

            insert_id = await session.insert_one(
                collection_name=cls.collection_name,
                document=document.dict(by_alias=True),
                user_id=user.id,
            )
            assert insert_id == document.id

            try:
                feature_namespace = await FeatureNamespaceController.get(
                    user=user,
                    persistent=session,
                    document_id=document.feature_namespace_id,
                )

                # update feature namespace
                await FeatureNamespaceController.update_feature_namespace(
                    user=user,
                    persistent=persistent,
                    feature_namespace_id=feature_namespace.id,
                    data=FeatureNamespaceUpdate(version_id=document.id),
                )

            except HTTPException as exc:
                if exc.status_code == HTTPStatus.NOT_FOUND:
                    # create a new feature namespace object
                    await FeatureNamespaceController.create_feature_namespace(
                        user=user,
                        persistent=session,
                        data=FeatureNamespaceCreate(
                            _id=document.feature_namespace_id,
                            name=document.name,
                            version_ids=[insert_id],
                            readiness=FeatureReadiness.DRAFT,
                            default_version_id=insert_id,
                            default_version_mode=DefaultVersionMode.AUTO,
                        ),
                    )
                else:
                    raise exc

            # insert feature registry into feature store
            feature_store_id, _ = data.tabular_source
            feature_store_dict = await cls.get_document(
                user=user,
                persistent=session,
                collection_name=FeatureStoreModel.collection_name(),
                document_id=feature_store_id,
                exception_status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            )
            feature_store = ExtendedFeatureStoreModel(**feature_store_dict)
            await cls._insert_feature_registry(user, document, feature_store, get_credential)

        return await cls.get(user=user, persistent=persistent, document_id=insert_id)
