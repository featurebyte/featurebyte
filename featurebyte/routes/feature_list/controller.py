"""
FeatureList API route controller
"""
from __future__ import annotations

from typing import Any

from http import HTTPStatus

from featurebyte.models.feature import FeatureListModel, FeatureModel
from featurebyte.persistent import Persistent
from featurebyte.routes.common.base import BaseController, GetType
from featurebyte.schema.feature_list import FeatureListCreate, FeatureListPaginatedList


class FeatureListController(BaseController[FeatureListModel, FeatureListPaginatedList]):
    """
    FeatureList controller
    """

    collection_name = FeatureListModel.collection_name()
    document_class = FeatureListModel
    paginated_document_class = FeatureListPaginatedList

    @classmethod
    async def create_feature_list(
        cls, user: Any, persistent: Persistent, data: FeatureListCreate
    ) -> FeatureListModel:
        """
        Create FeatureList at persistent (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that feature list will be saved to
        data: FeatureListCreate
            Feature creation payload

        Returns
        -------
        FeatureListModel
            Newly created feature list object
        """
        document = FeatureListModel(**data.json_dict(), user_id=user.id)

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
                user_id=user.id,
            )

        # check whether the feature(s) in the feature list saved to persistent or not
        for feature_id in document.feature_ids:
            await cls.get_document(
                user=user,
                persistent=persistent,
                collection_name=FeatureModel.collection_name(),
                document_id=feature_id,
                exception_status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            )

        insert_id = await persistent.insert_one(
            collection_name=cls.collection_name,
            document=document.dict(by_alias=True),
            user_id=user.id,
        )
        assert insert_id == document.id

        return await cls.get(user=user, persistent=persistent, document_id=insert_id)
