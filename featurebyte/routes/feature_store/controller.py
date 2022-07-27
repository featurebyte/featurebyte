"""
FeatureStore API route controller
"""
from __future__ import annotations

from typing import Any

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.enum import CollectionName
from featurebyte.persistent import Persistent
from featurebyte.routes.common.util import get_utc_now
from featurebyte.schema.feature_store import FeatureStore, FeatureStoreCreate


class FeatureStoreController:
    """
    FeatureStore controller
    """

    collection_name = CollectionName.FEATURE_STORE

    @classmethod
    async def create_feature_store(
        cls,
        user: Any,
        persistent: Persistent,
        data: FeatureStoreCreate,
    ) -> FeatureStore:
        """
        Create Feature Store at persistent

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that entity will be saved to
        data: FeatureStoreCreate
            FeatureStore creation payload

        Returns
        -------
        FeatureStore
            Newly created feature store object

        Raises
        ------
        HTTPException
            If the feature store name conflicts with existing feature store name
        """

        document = FeatureStore(
            **data.dict(by_alias=True),
            user_id=user.id,
            created_at=get_utc_now(),
        )

        conflict_feature_store = await persistent.find_one(
            collection_name=cls.collection_name, query_filter={"_id": data.id}
        )
        if conflict_feature_store:
            raise HTTPException(
                status_code=HTTPStatus.CONFLICT,
                detail=f'FeatureStore name (feature_store.id: "{data.id}") already exists.',
            )

        conflict_feature_store = await persistent.find_one(
            collection_name=cls.collection_name, query_filter={"name": data.name}
        )
        if conflict_feature_store:
            raise HTTPException(
                status_code=HTTPStatus.CONFLICT,
                detail=f'FeatureStore name (feature_store.name: "{data.name}") already exists.',
            )

        insert_id = await persistent.insert_one(
            collection_name=cls.collection_name, document=document.dict(by_alias=True)
        )
        assert insert_id == document.id
        return document

    @classmethod
    async def retrieve_feature_store(
        cls,
        user: Any,
        persistent: Persistent,
        feature_store_id: ObjectId,
    ) -> FeatureStore:
        """
        Retrieve feature store given feature store identifier (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that entity will be saved to
        feature_store_id: ObjectId
            FeatureStore ID

        Returns
        -------
        FeatureStore
            FeatureStore object which matches given feature store id

        Raises
        ------
        HTTPException
            If the feature store not found
        """
        query_filter = {"_id": ObjectId(feature_store_id), "user_id": user.id}
        feature_store = await persistent.find_one(
            collection_name=cls.collection_name, query_filter=query_filter
        )
        if feature_store is None:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail=(f'FeatureStore (feature_store.id: "{feature_store_id}") not found! '),
            )
        return FeatureStore(**feature_store)
