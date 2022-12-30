"""
Feature API route controller
"""
from __future__ import annotations

from typing import Any, Literal, Union

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi.exceptions import HTTPException

from featurebyte.models.feature import FeatureModel, FeatureReadiness
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.feature import (
    FeatureCreate,
    FeatureNewVersionCreate,
    FeaturePaginatedList,
    FeaturePreview,
    FeatureSQL,
    FeatureUpdate,
)
from featurebyte.schema.info import FeatureInfo
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_readiness import FeatureReadinessService
from featurebyte.service.info import InfoService
from featurebyte.service.preview import PreviewService
from featurebyte.service.version import VersionService


class FeatureController(BaseDocumentController[FeatureModel, FeatureService, FeaturePaginatedList]):
    """
    Feature controller
    """

    paginated_document_class = FeaturePaginatedList

    def __init__(
        self,
        service: FeatureService,
        feature_list_service: FeatureListService,
        feature_readiness_service: FeatureReadinessService,
        preview_service: PreviewService,
        version_service: VersionService,
        info_service: InfoService,
    ):
        super().__init__(service)
        self.feature_list_service = feature_list_service
        self.feature_readiness_service = feature_readiness_service
        self.preview_service = preview_service
        self.version_service = version_service
        self.info_service = info_service

    async def create_feature(
        self, data: Union[FeatureCreate, FeatureNewVersionCreate]
    ) -> FeatureModel:
        """
        Create Feature at persistent (GitDB or MongoDB)

        Parameters
        ----------
        data: FeatureCreate | FeatureNewVersionCreate
            Feature creation payload

        Returns
        -------
        FeatureModel
            Newly created feature object
        """
        if isinstance(data, FeatureCreate):
            document = await self.service.create_document(data=data)
        else:
            document = await self.version_service.create_new_feature_version(data=data)

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

    async def preview(self, feature_preview: FeaturePreview, get_credential: Any) -> dict[str, Any]:
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
        dict[str, Any]
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

    async def get_info(
        self,
        document_id: ObjectId,
        verbose: bool,
    ) -> FeatureInfo:
        """
        Get document info given document ID

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Flag to control verbose level

        Returns
        -------
        InfoDocument
        """
        info_document = await self.info_service.get_feature_info(
            document_id=document_id, verbose=verbose
        )
        return info_document

    async def sql(self, feature_sql: FeatureSQL) -> str:
        """
        Get Feature SQL

        Parameters
        ----------
        feature_sql: FeatureSQL
            FeatureSQL object

        Returns
        -------
        str
            Dataframe converted to json string
        """
        return await self.preview_service.feature_sql(feature_sql=feature_sql)
