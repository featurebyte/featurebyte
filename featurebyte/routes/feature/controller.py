"""
Feature API route controller
"""
from __future__ import annotations

from typing import Any, Literal, Union, cast

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi.exceptions import HTTPException

from featurebyte.models.feature import FeatureModel, FeatureReadiness
from featurebyte.routes.common.base import BaseDocumentController, GetInfoControllerMixin
from featurebyte.schema.feature import (
    FeatureCreate,
    FeatureInfo,
    FeaturePaginatedList,
    FeaturePreview,
    FeatureUpdate,
    VersionCreate,
)
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_readiness import FeatureReadinessService
from featurebyte.service.online_enable import OnlineEnableService
from featurebyte.service.preview import PreviewService
from featurebyte.service.version import VersionService


class FeatureController(  # type: ignore[misc]
    BaseDocumentController[FeatureModel, FeaturePaginatedList], GetInfoControllerMixin[FeatureInfo]
):
    """
    Feature controller
    """

    paginated_document_class = FeaturePaginatedList

    def __init__(
        self,
        service: FeatureService,
        feature_list_service: FeatureListService,
        feature_readiness_service: FeatureReadinessService,
        online_enable_service: OnlineEnableService,
        preview_service: PreviewService,
        version_service: VersionService,
    ):
        super().__init__(service)  # type: ignore[arg-type]
        self.feature_list_service = feature_list_service
        self.feature_readiness_service = feature_readiness_service
        self.online_enable_service = online_enable_service
        self.preview_service = preview_service
        self.version_service = version_service

    async def create_feature(
        self, get_credential: Any, data: Union[FeatureCreate, VersionCreate]
    ) -> FeatureModel:
        """
        Create Feature at persistent (GitDB or MongoDB)

        Parameters
        ----------
        get_credential: Any
            Get credential handler function
        data: FeatureCreate | VersionCreate
            Feature creation payload

        Returns
        -------
        FeatureModel
            Newly created feature object
        """
        if isinstance(data, FeatureCreate):
            document = cast(
                FeatureModel,
                await self.service.create_document(data=data, get_credential=get_credential),
            )
        else:
            document = await self.version_service.create_new_feature_version(
                data=data, get_credential=get_credential
            )

        # update feature namespace readiness due to introduction of new feature
        await self.feature_readiness_service.update_feature_namespace(
            feature_namespace_id=document.feature_namespace_id,
            return_document=False,
        )
        return document

    async def update_feature(
        self,
        feature_id: ObjectId,
        data: FeatureUpdate,
    ) -> FeatureModel:
        """
        Update Feature at persistent

        Parameters
        ----------
        feature_id: ObjectId
            Feature ID
        data: FeatureUpdate
            Feature update payload

        Returns
        -------
        FeatureModel
            Feature object with updated attribute(s)
        """
        if data.readiness:
            await self.feature_readiness_service.update_feature(
                feature_id=feature_id,
                readiness=FeatureReadiness(data.readiness),
                return_document=False,
            )
        if data.online_enabled is not None:
            await self.online_enable_service.update_feature(
                feature_id=feature_id,
                online_enabled=data.online_enabled,
                return_document=False,
            )
        return await self.get(document_id=feature_id)

    async def list_features(
        self,
        page: int = 1,
        page_size: int = 10,
        sort_by: str | None = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
        **kwargs: Any,
    ) -> FeaturePaginatedList:
        """
        List documents stored at persistent (GitDB or MongoDB)

        Parameters
        ----------
        page: int
            Page number
        page_size: int
            Number of items per page
        sort_by: str | None
            Key used to sort the returning documents
        sort_dir: "asc" or "desc"
            Sorting the returning documents in ascending order or descending order
        kwargs: Any
            Additional keyword arguments

        Returns
        -------
        FeaturePaginatedList
            List of documents fulfilled the filtering condition
        """
        params = kwargs.copy()
        feature_list_id = params.pop("feature_list_id")
        if feature_list_id:
            feature_list_document = await self.feature_list_service.get_document(
                document_id=feature_list_id
            )
            params["query_filter"] = {"_id": {"$in": feature_list_document.feature_ids}}

        feature_namespace_id = params.pop("feature_namespace_id")
        if feature_namespace_id:
            query_filter = params.get("query_filter", {}).copy()
            query_filter["feature_namespace_id"] = feature_namespace_id
            params["query_filter"] = query_filter

        return await self.list(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            **params,
        )

    async def preview(self, feature_preview: FeaturePreview, get_credential: Any) -> str:
        """
        Preview a Feature

        Parameters
        ----------
        feature_preview: FeaturePreview
            FeaturePreview object
        get_credential: Any
            Get credential handler function

        Returns
        -------
        str
            Dataframe converted to json string

        Raises
        ------
        HTTPException
            Invalid request payload
        """
        try:
            return await self.preview_service.preview_feature(
                feature_preview=feature_preview, get_credential=get_credential
            )
        except KeyError as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=exc.args[0]
            ) from exc
