"""
Feature API route controller
"""
from __future__ import annotations

from typing import Any, Literal, Type

from bson.objectid import ObjectId

from featurebyte.models.feature import FeatureModel
from featurebyte.persistent import Persistent
from featurebyte.routes.common.base import BaseDocumentController, GetInfoControllerMixin
from featurebyte.schema.feature import (
    FeatureCreate,
    FeatureInfo,
    FeaturePaginatedList,
    FeatureUpdate,
)
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService


class FeatureController(
    BaseDocumentController[FeatureModel, FeaturePaginatedList], GetInfoControllerMixin[FeatureInfo]
):
    """
    Feature controller
    """

    paginated_document_class = FeaturePaginatedList
    document_service_class: Type[FeatureService] = FeatureService  # type: ignore[assignment]

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
        document = await cls.document_service_class(
            user=user, persistent=persistent
        ).create_document(data=data, get_credential=get_credential)
        return document

    @classmethod
    async def update_feature(
        cls,
        user: Any,
        persistent: Persistent,
        feature_id: ObjectId,
        data: FeatureUpdate,
    ) -> FeatureModel:
        """
        Update Feature at persistent

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that entity will be saved to
        feature_id: ObjectId
            Feature ID
        data: FeatureUpdate
            Feature update payload

        Returns
        -------
        FeatureModel
            Feature object with updated attribute(s)
        """
        document = await cls.document_service_class(
            user=user, persistent=persistent
        ).update_document(document_id=feature_id, data=data)
        return document

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
    ) -> FeaturePaginatedList:
        params = kwargs.copy()
        feature_list_id = params.pop("feature_list_id")
        if feature_list_id:
            feature_list_document = await FeatureListService(
                user=user, persistent=persistent
            ).get_document(document_id=feature_list_id)
            params["query_filter"] = {"_id": {"$in": feature_list_document.feature_ids}}

        feature_namespace_id = params.pop("feature_namespace_id")
        if feature_namespace_id:
            query_filter = params.get("query_filter", {}).copy()
            query_filter["feature_namespace_id"] = feature_namespace_id
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
