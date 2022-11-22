"""
FeatureList API route controller
"""
from __future__ import annotations

from typing import Any, Literal, Union

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import UploadFile
from fastapi.exceptions import HTTPException
from fastapi.responses import StreamingResponse

from featurebyte.common.utils import dataframe_from_arrow_stream
from featurebyte.exception import MissingPointInTimeColumnError, TooRecentPointInTimeError
from featurebyte.models.feature import FeatureReadiness
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.feature_list import (
    FeatureListCreate,
    FeatureListGetHistoricalFeatures,
    FeatureListNewVersionCreate,
    FeatureListPaginatedList,
    FeatureListPreview,
    FeatureListSQL,
    FeatureListUpdate,
)
from featurebyte.schema.info import FeatureListInfo
from featurebyte.service.deploy import DeployService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_readiness import FeatureReadinessService
from featurebyte.service.info import InfoService
from featurebyte.service.preview import PreviewService
from featurebyte.service.version import VersionService


class FeatureListController(
    BaseDocumentController[FeatureListModel, FeatureListService, FeatureListPaginatedList]
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
        version_service: VersionService,
        info_service: InfoService,
    ):
        super().__init__(service)
        self.feature_readiness_service = feature_readiness_service
        self.deploy_service = deploy_service
        self.preview_service = preview_service
        self.version_service = version_service
        self.info_service = info_service

    async def create_feature_list(
        self, get_credential: Any, data: Union[FeatureListCreate, FeatureListNewVersionCreate]
    ) -> FeatureListModel:
        """
        Create FeatureList at persistent (GitDB or MongoDB)

        Parameters
        ----------
        get_credential: Any
            Get credential handler function
        data: FeatureListCreate | FeatureListNewVersionCreate
            Feature list creation payload

        Returns
        -------
        FeatureListModel
            Newly created feature list object
        """
        if isinstance(data, FeatureListCreate):
            document = await self.service.create_document(data=data, get_credential=get_credential)
        else:
            document = await self.version_service.create_new_feature_list_version(
                data=data, get_credential=get_credential
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
        get_credential: Any,
    ) -> FeatureListModel:
        """
        Update FeatureList at persistent

        Parameters
        ----------
        feature_list_id: ObjectId
            FeatureList ID
        data: FeatureListUpdate
            FeatureList update payload
        get_credential: Any
            Get credential handler function

        Returns
        -------
        FeatureListModel
            FeatureList object with updated attribute(s)
        """
        if data.make_production_ready:
            feature_list = await self.get(document_id=feature_list_id)
            for feature_id in feature_list.feature_ids:
                await self.feature_readiness_service.update_feature(
                    feature_id=feature_id,
                    readiness=FeatureReadiness.PRODUCTION_READY,
                    return_document=False,
                )
        if data.deployed is not None:
            await self.deploy_service.update_feature_list(
                feature_list_id=feature_list_id,
                deployed=data.deployed,
                get_credential=get_credential,
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
        Preview a Feature List

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

    async def get_historical_features(
        self,
        training_events: UploadFile,
        featurelist_get_historical_features: FeatureListGetHistoricalFeatures,
        get_credential: Any,
    ) -> StreamingResponse:
        """
        Get historical features for Feature List

        Parameters
        ----------
        training_events: UploadFile
            Uploaded file
        featurelist_get_historical_features: FeatureListGetHistoricalFeatures
            FeatureListGetHistoricalFeatures object
        get_credential: Any
            Get credential handler function

        Returns
        -------
        StreamingResponse
            StreamingResponse object

        Raises
        ------
        HTTPException
            Invalid request payload
        """
        try:
            bytestream = await self.preview_service.get_historical_features(
                training_events=dataframe_from_arrow_stream(training_events.file),
                featurelist_get_historical_features=featurelist_get_historical_features,
                get_credential=get_credential,
            )
        except (MissingPointInTimeColumnError, TooRecentPointInTimeError) as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=exc.args[0]
            ) from exc
        return StreamingResponse(
            bytestream,
            media_type="application/octet-stream",
        )

    async def get_info(
        self,
        document_id: ObjectId,
        verbose: bool,
    ) -> FeatureListInfo:
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
        info_document = await self.info_service.get_feature_list_info(
            document_id=document_id, verbose=verbose
        )
        return info_document

    async def sql(self, featurelist_sql: FeatureListSQL) -> str:
        """
        Preview a Feature List

        Parameters
        ----------
        featurelist_sql: FeatureListSQL
            FeatureListSQL object

        Returns
        -------
        str
            SQL statements
        """
        return await self.preview_service.featurelist_sql(featurelist_sql=featurelist_sql)

    async def get_historical_features_sql(
        self,
        training_events: UploadFile,
        featurelist_get_historical_features: FeatureListGetHistoricalFeatures,
    ) -> str:
        """
        Get historical features sql for Feature List

        Parameters
        ----------
        training_events: UploadFile
            Uploaded file
        featurelist_get_historical_features: FeatureListGetHistoricalFeatures
            FeatureListGetHistoricalFeatures object

        Returns
        -------
        str
            SQL statements

        Raises
        ------
        HTTPException
            Invalid request payload
        """
        try:
            return await self.preview_service.get_historical_features_sql(
                training_events=dataframe_from_arrow_stream(training_events.file),
                featurelist_get_historical_features=featurelist_get_historical_features,
            )
        except (MissingPointInTimeColumnError, TooRecentPointInTimeError) as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=exc.args[0]
            ) from exc
