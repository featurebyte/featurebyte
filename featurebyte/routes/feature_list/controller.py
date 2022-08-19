"""
FeatureList API route controller
"""
from __future__ import annotations

from typing import Any, List, Optional

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.enum import SourceType
from featurebyte.exception import DuplicatedRegistryError
from featurebyte.feature_manager.model import ExtendedFeatureListModel, FeatureSignature
from featurebyte.feature_manager.snowflake_feature_list import FeatureListManagerSnowflake
from featurebyte.models.feature import FeatureListModel, FeatureModel, FeatureReadiness
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.persistent import Persistent
from featurebyte.routes.common.base import BaseController
from featurebyte.schema.feature_list import FeatureListCreate, FeatureListPaginatedList


class FeatureListController(BaseController[FeatureListModel, FeatureListPaginatedList]):
    """
    FeatureList controller
    """

    collection_name = FeatureListModel.collection_name()
    document_class = FeatureListModel
    paginated_document_class = FeatureListPaginatedList

    @classmethod
    async def _insert_feature_list_registry(
        cls,
        user: Any,
        document: ExtendedFeatureListModel,
        feature_store: ExtendedFeatureStoreModel,
        get_credential: Any,
    ) -> None:
        """
        Insert feature list registry into feature list store

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        document: ExtendedFeatureListModel
            ExtendedFeatureList document
        feature_store: ExtendedFeatureStoreModel
            FeatureStore document
        get_credential: Any
            Get credential handler function

        Raises
        ------
        HTTPException
            When the feature registry already exists at the feature store
        """
        if feature_store.type == SourceType.SNOWFLAKE:
            db_session = feature_store.get_session(
                credentials={
                    feature_store.name: await get_credential(
                        user_id=user.id, feature_store_name=feature_store.name
                    )
                }
            )
            feature_list_manager = FeatureListManagerSnowflake(session=db_session)
            try:
                feature_list_manager.insert_feature_list_registry(document)
            except DuplicatedRegistryError as exc:
                # someone else already registered the feature at snowflake
                # do not remove the current registry & raise error to remove persistent record
                raise HTTPException(
                    status_code=HTTPStatus.CONFLICT,
                    detail=(
                        f'FeatureList (name: "{document.name}") has been registered by '
                        f"other feature list at Snowflake feature list store."
                    ),
                ) from exc

    @classmethod
    async def create_feature_list(
        cls, user: Any, persistent: Persistent, get_credential: Any, data: FeatureListCreate
    ) -> FeatureListModel:
        """
        Create FeatureList at persistent (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that feature list will be saved to
        get_credential: Any
            Get credential handler function
        data: FeatureListCreate
            Feature creation payload

        Returns
        -------
        FeatureListModel
            Newly created feature list object

        Raises
        ------
        HTTPException
            When not all features share the same feature store
        """
        # sort feature_ids before saving to persistent storage to ease feature_ids comparison in uniqueness check
        document = FeatureListModel(
            **{**data.json_dict(), "feature_ids": sorted(data.feature_ids), "user_id": user.id}
        )

        async with persistent.start_transaction() as session:
            # check any conflict with existing documents
            await cls.check_document_unique_constraints(
                persistent=persistent,
                user_id=user.id,
                document=document,
            )

            # check whether the feature(s) in the feature list saved to persistent or not
            feature_store_id: Optional[ObjectId] = None
            feature_signatures: List[FeatureSignature] = []
            feature_list_readiness: FeatureReadiness = FeatureReadiness.PRODUCTION_READY
            for feature_id in document.feature_ids:
                feature_dict = await cls.get_document(
                    user=user,
                    persistent=session,
                    collection_name=FeatureModel.collection_name(),
                    document_id=feature_id,
                    exception_status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                )
                feature = FeatureModel(**feature_dict)
                feature_list_readiness = min(
                    feature_list_readiness, FeatureReadiness(feature.readiness)
                )
                feature_signatures.append(
                    FeatureSignature(id=feature.id, name=feature.name, version=feature.version)
                )
                if feature_store_id and (feature_store_id != feature.tabular_source[0]):
                    raise HTTPException(
                        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                        detail=(
                            "All the Feature objects within the same FeatureList object must be from the same "
                            "feature store."
                        ),
                    )

                # store previous feature store id
                feature_store_id = feature.tabular_source[0]

            # update document with readiness
            document = FeatureListModel(
                **{**document.dict(by_alias=True), "readiness": feature_list_readiness}
            )
            feature_store_dict = await cls.get_document(
                user=user,
                persistent=session,
                collection_name=FeatureStoreModel.collection_name(),
                document_id=ObjectId(feature_store_id),
                exception_status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            )
            feature_store = ExtendedFeatureStoreModel(**feature_store_dict)

            insert_id = await session.insert_one(
                collection_name=cls.collection_name,
                document=document.dict(by_alias=True),
                user_id=user.id,
            )
            assert insert_id == document.id

            # insert feature list registry into feature list store
            await cls._insert_feature_list_registry(
                user=user,
                document=ExtendedFeatureListModel(
                    **document.dict(by_alias=True), features=feature_signatures
                ),
                feature_store=feature_store,
                get_credential=get_credential,
            )

        return await cls.get(user=user, persistent=persistent, document_id=insert_id)
