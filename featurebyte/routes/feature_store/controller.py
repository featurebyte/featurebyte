"""
FeatureStore API route controller
"""
from __future__ import annotations

from typing import Any, Literal, Optional

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.enum import CollectionName
from featurebyte.persistent import Persistent
from featurebyte.routes.common.util import get_utc_now
from featurebyte.schema.feature_store import FeatureStore, FeatureStoreCreate, FeatureStoreList


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
            Object that feature store will be saved to
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

        # check id conflict
        conflict_feature_store = await persistent.find_one(
            collection_name=cls.collection_name, query_filter={"_id": data.id}
        )
        if conflict_feature_store:
            raise HTTPException(
                status_code=HTTPStatus.CONFLICT,
                detail=f'FeatureStore name (feature_store.id: "{data.id}") already exists.',
            )

        # check name conflict
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
    async def get_feature_store(
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
            Object that feature store will be saved to
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
                detail=f'FeatureStore (feature_store.id: "{feature_store_id}") not found! ',
            )
        return FeatureStore(**feature_store)

    @classmethod
    async def list_feature_stores(
        cls,
        user: Any,
        persistent: Persistent,
        page: int = 1,
        page_size: int = 10,
        sort_by: str | None = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
        name: Optional[str] = None,
    ) -> FeatureStoreList:
        """
        List FeatureStores stored at persistent (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that feature store will be saved to
        page: int
            Page number
        page_size: int
            Number of items per page
        sort_by: str | None
            Key used to sort the returning feature stores
        sort_dir: "asc" or "desc"
            Sorting the returning feature stores in ascending order or descending order
        name: str | None
            FeatureStore name used to filter the feature stores

        Returns
        -------
        FeatureStoreList
            List of feature stores fulfilled the filtering condition
        """
        query_filter = {"user_id": user.id}
        if name is not None:
            query_filter["name"] = name
        docs, total = await persistent.find(
            collection_name=cls.collection_name,
            query_filter=query_filter,
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=page,
            page_size=page_size,
        )
        return FeatureStoreList(page=page, page_size=page_size, total=total, data=list(docs))
