"""
FeatureNamespace API route controller
"""
from __future__ import annotations

from typing import Any

from featurebyte.models.feature import FeatureNamespaceModel
from featurebyte.persistent import Persistent
from featurebyte.routes.common.base import BaseController
from featurebyte.schema.feature_namespace import FeatureNamespaceCreate, FeatureNamespaceList


class FeatureNamespaceController(BaseController[FeatureNamespaceModel, FeatureNamespaceList]):
    """
    FeatureName controller
    """

    collection_name = FeatureNamespaceModel.collection_name()
    document_class = FeatureNamespaceModel
    paginated_document_class = FeatureNamespaceList

    @classmethod
    async def create_feature_namespace(
        cls, user: Any, persistent: Persistent, data: FeatureNamespaceCreate
    ) -> FeatureNamespaceModel:
        """
        Create Feature Namespace at persistent

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that feature namespace will be saved to
        data: FeatureNamespaceCreate
            FeatureNamespace creation payload

        Returns
        -------
        FeatureNamespaceModel
            Newly created feature store document
        """
        document = FeatureNamespaceModel(**data.json_dict(), user_id=user.id)
        # check any conflict with existing documents
        await cls.check_document_unique_constraints(
            persistent=persistent,
            user_id=user.id,
            document=document,
        )
        insert_id = await persistent.insert_one(
            collection_name=cls.collection_name,
            document=document.dict(by_alias=True),
            user_id=user.id,
        )
        assert insert_id == document.id
        return await cls.get(user=user, persistent=persistent, document_id=insert_id)
