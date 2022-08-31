"""
FeatureStore API route controller
"""
from __future__ import annotations

from typing import Any

from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.persistent.base import Persistent
from featurebyte.routes.common.base import BaseController
from featurebyte.routes.common.operation import DictProject, DictTransform
from featurebyte.schema.feature_store import FeatureStoreCreate, FeatureStoreList


class FeatureStoreController(BaseController[FeatureStoreModel, FeatureStoreList]):
    """
    FeatureStore controller
    """

    collection_name = FeatureStoreModel.collection_name()
    document_class = FeatureStoreModel
    paginated_document_class = FeatureStoreList
    info_transform = DictTransform(
        rule={
            **BaseController.base_info_transform_rule,
            "__root__": DictProject(rule=["type", "details"]),
        }
    )

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
        # pylint: disable=duplicate-code
        document = FeatureStoreModel(**data.json_dict(), user_id=user.id)

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
