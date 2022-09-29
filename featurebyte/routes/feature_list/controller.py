"""
FeatureList API route controller
"""
from __future__ import annotations

from typing import Any, Literal, cast

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi.exceptions import HTTPException

from featurebyte.models.feature_list import FeatureListModel
from featurebyte.routes.common.base import BaseDocumentController, GetInfoControllerMixin
from featurebyte.schema.feature_list import (
    FeatureListCreate,
    FeatureListInfo,
    FeatureListPaginatedList,
    FeatureListPreview,
    FeatureListUpdate,
)
from featurebyte.service.deploy import DeployService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_readiness import FeatureReadinessService
from featurebyte.service.preview import PreviewService


class FeatureListController(  # type: ignore[misc]
    BaseDocumentController[FeatureListModel, FeatureListPaginatedList],
    GetInfoControllerMixin[FeatureListInfo],
):
    """
    FeatureList controller
    """

    paginated_document_class = FeatureListPaginatedList

    def __init__(
        self,
        service: FeatureListService,
        feature_readiness_service: FeatureReadinessService,
        deploy_service: DeployService,
        preview_service: PreviewService,
    ):
        super().__init__(service)  # type: ignore[arg-type]
        self.feature_readiness_service = feature_readiness_service
        self.deploy_service = deploy_service
        self.preview_service = preview_service

    async def create_feature_list(
        self, get_credential: Any, data: FeatureListCreate
    ) -> FeatureListModel:
        """
        Create FeatureList at persistent (GitDB or MongoDB)

        Parameters
        ----------
        get_credential: Any
            Get credential handler function
        data: FeatureListCreate
            Feature list creation payload

        Returns
        -------
        FeatureListModel
            Newly created feature list object
        """
        document = cast(
            FeatureListModel,
            await self.service.create_document(data=data, get_credential=get_credential),
        )

        # update feature namespace readiness due to introduction of new feature list
        await self.feature_readiness_service.update_feature_list_namespace(
            feature_list_namespace_id=document.feature_list_namespace_id,
            return_document=False,
        )
        return document

    async def update_feature_list(
        self,
        feature_list_id: ObjectId,
        data: FeatureListUpdate,
    ) -> FeatureListModel:
        """
        Update FeatureList at persistent

        Parameters
        ----------
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
            await self.deploy_service.update_feature_list(
                feature_list_id=feature_list_id,
                deployed=data.deployed,
                return_document=False,
            )
        return await self.get(document_id=feature_list_id)

    async def list_feature_lists(
        self,
        page: int = 1,
        page_size: int = 10,
        sort_by: str | None = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
        **kwargs: Any,
    ) -> FeatureListPaginatedList:
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
        FeatureListPaginatedList
            List of documents fulfilled the filtering condition
        """
        params = kwargs.copy()
        feature_list_namespace_id = params.pop("feature_list_namespace_id")
        if feature_list_namespace_id:
            query_filter = params.get("query_filter", {}).copy()
            query_filter["feature_list_namespace_id"] = feature_list_namespace_id
            params["query_filter"] = query_filter

        return await self.list(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            **params,
        )

    async def preview(self, featurelist_preview: FeatureListPreview, get_credential: Any) -> str:
        """
        Preview a Feature

        Parameters
        ----------
        featurelist_preview: FeatureListPreview
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
            return await self.preview_service.preview_featurelist(
                featurelist_preview=featurelist_preview, get_credential=get_credential
            )
        except KeyError as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=exc.args[0]
            ) from exc
