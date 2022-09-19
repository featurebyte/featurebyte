"""
FeatureList API route controller
"""
from __future__ import annotations

from typing import Any, Literal, Type

from bson.objectid import ObjectId

from featurebyte.models.feature_list import FeatureListModel
from featurebyte.persistent import Persistent
from featurebyte.routes.common.base import BaseDocumentController, GetInfoControllerMixin
from featurebyte.schema.feature_list import (
    FeatureListCreate,
    FeatureListInfo,
    FeatureListPaginatedList,
    FeatureListUpdate,
)
from featurebyte.service.deploy import DeployService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_readiness import FeatureReadinessService


class FeatureListController(
    BaseDocumentController[FeatureListModel, FeatureListPaginatedList],
    GetInfoControllerMixin[FeatureListInfo],
):
    """
    FeatureList controller
    """

    paginated_document_class = FeatureListPaginatedList
    document_service_class: Type[FeatureListService] = FeatureListService  # type: ignore[assignment]

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
            Feature list creation payload

        Returns
        -------
        FeatureListModel
            Newly created feature list object
        """
        document = await cls.document_service_class(
            user=user, persistent=persistent
        ).create_document(data=data, get_credential=get_credential)

        # update feature namespace readiness due to introduction of new feature list
        readiness_service = FeatureReadinessService(user=user, persistent=persistent)
        await readiness_service.update_feature_list_namespace(
            feature_list_namespace_id=document.feature_list_namespace_id,
            return_document=False,
        )
        return document

    @classmethod
    async def update_feature_list(
        cls,
        user: Any,
        persistent: Persistent,
        feature_list_id: ObjectId,
        data: FeatureListUpdate,
    ) -> FeatureListModel:
        """
        Update FeatureList at persistent

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that entity will be saved to
        feature_list_id: ObjectId
            FeatureList ID
        data: FeatureListUpdate
            FeatureList update payload

        Returns
        -------
        FeatureListModel
            FeatureList object with updated attribute(s)
        """
        if data.deployed is not None:
            deploy_service = DeployService(user=user, persistent=persistent)
            await deploy_service.update_feature_list(
                feature_list_id=feature_list_id,
                deployed=data.deployed,
                return_document=False,
            )
        return await cls.get(user=user, persistent=persistent, document_id=feature_list_id)

    @classmethod
    async def list(
        cls,
        user: Any,
        persistent: Persistent,
        page: int = 1,
        page_size: int = 10,
        sort_by: str | None = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
        **kwargs: Any,
    ) -> FeatureListPaginatedList:
        params = kwargs.copy()
        feature_list_namespace_id = params.pop("feature_list_namespace_id")
        if feature_list_namespace_id:
            query_filter = params.get("query_filter", {}).copy()
            query_filter["feature_list_namespace_id"] = feature_list_namespace_id
            params["query_filter"] = query_filter

        return await super().list(
            user=user,
            persistent=persistent,
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            **params,
        )
