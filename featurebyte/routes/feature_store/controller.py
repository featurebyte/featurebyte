"""
FeatureStore API route controller
"""
from __future__ import annotations

from typing import Any

from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.persistent.base import Persistent
from featurebyte.routes.common.base import BaseController, GetType
from featurebyte.schema.feature_store import FeatureStoreCreate, FeatureStoreList


class FeatureStoreController(BaseController[FeatureStoreModel, FeatureStoreList]):
    """
    FeatureStore controller
    """

    collection_name = FeatureStoreModel.collection_name()
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
        """

        document = FeatureStoreModel(**data.json_dict(), user_id=user.id)

        # check any conflict with existing documents
        constraints_check_triples: list[tuple[dict[str, Any], dict[str, Any], GetType]] = [
            ({"_id": data.id}, {"id": data.id}, "name"),
            ({"name": data.name}, {"name": data.name}, "name"),
        ]
        for query_filter, doc_represent, get_type in constraints_check_triples:
            await cls.check_document_creation_conflict(
                persistent=persistent,
                query_filter=query_filter,
                doc_represent=doc_represent,
                get_type=get_type,
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
