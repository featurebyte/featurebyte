"""
FeatureStore API route controller
"""
from __future__ import annotations

from typing import Any

from http import HTTPStatus

from fastapi import HTTPException

from featurebyte.enum import CollectionName
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.persistent.base import Persistent
from featurebyte.routes.common.base import BaseController
from featurebyte.routes.common.util import get_utc_now
from featurebyte.schema.feature_store import FeatureStoreCreate, FeatureStoreList


class FeatureStoreController(BaseController[FeatureStoreModel, FeatureStoreList]):
    """
    FeatureStore controller
    """

    collection_name = CollectionName.FEATURE_STORE
    document_class = FeatureStoreModel
    paginated_document_class = FeatureStoreList

    @classmethod
    async def create_feature_store(
        cls,
        user: Any,
        persistent: Persistent,
        data: FeatureStoreCreate,
    ) -> FeatureStoreModel:
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
        FeatureStoreModel
            Newly created feature store document

        Raises
        ------
        HTTPException
            If the feature store name conflicts with existing feature store name
        """

        document = FeatureStoreModel(
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
                detail=f'FeatureStore (feature_store.id: "{data.id}") already exists.',
            )

        # check name conflict
        conflict_feature_store = await persistent.find_one(
            collection_name=cls.collection_name, query_filter={"name": data.name}
        )
        if conflict_feature_store:
            raise HTTPException(
                status_code=HTTPStatus.CONFLICT,
                detail=f'FeatureStore (feature_store.name: "{data.name}") already exists.',
            )

        insert_id = await persistent.insert_one(
            collection_name=cls.collection_name, document=document.dict(by_alias=True)
        )
        assert insert_id == document.id
        return await cls.get(
            user=user,
            persistent=persistent,
            document_id=insert_id,
        )
