"""
FeatureList API route controller
"""
from __future__ import annotations

from typing import Any, Dict, Literal, Optional, Union

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import UploadFile
from fastapi.exceptions import HTTPException
from fastapi.responses import StreamingResponse

from featurebyte.common.utils import dataframe_from_arrow_stream
from featurebyte.exception import (
    FeatureListNotOnlineEnabledError,
    MissingPointInTimeColumnError,
    RequiredEntityNotProvidedError,
    TooRecentPointInTimeError,
    UnexpectedServingNamesMappingError,
)
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.feature import FeatureReadiness
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.feature_list import (
    FeatureListCreate,
    FeatureListGetHistoricalFeatures,
    FeatureListGetOnlineFeatures,
    FeatureListNewVersionCreate,
    FeatureListPaginatedList,
    FeatureListPreview,
    FeatureListSQL,
    FeatureListUpdate,
    OnlineFeaturesResponseModel,
)
from featurebyte.schema.info import FeatureListInfo
from featurebyte.schema.task import Task
from featurebyte.schema.worker.task.feature_list_deploy import FeatureListDeployTaskPayload
from featurebyte.service.deploy import DeployService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_readiness import FeatureReadinessService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.info import InfoService
from featurebyte.service.online_serving import OnlineServingService
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
        online_serving_service: OnlineServingService,
        feature_store_warehouse_service: FeatureStoreWarehouseService,
        feature_service: FeatureService,
        task_controller: TaskController,
    ):
        super().__init__(service)
        self.feature_readiness_service = feature_readiness_service
        self.deploy_service = deploy_service
        self.preview_service = preview_service
        self.version_service = version_service
        self.info_service = info_service
        self.online_serving_service = online_serving_service
        self.feature_store_warehouse_service = feature_store_warehouse_service
        self.feature_service = feature_service
        self.task_controller = task_controller

    async def create_feature_list(
        self, data: Union[FeatureListCreate, FeatureListNewVersionCreate]
    ) -> FeatureListModel:
        """
        Create FeatureList at persistent (GitDB or MongoDB)

        Parameters
        ----------
        data: FeatureListCreate | FeatureListNewVersionCreate
            Feature list creation payload

        Returns
        -------
        FeatureListModel
            Newly created feature list object
        """
        if isinstance(data, FeatureListCreate):
            document = await self.service.create_document(data=data)
        else:
            document = await self.version_service.create_new_feature_list_version(data=data)

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
        if data.make_production_ready:
            feature_list = await self.get(document_id=feature_list_id)
            for feature_id in feature_list.feature_ids:
                await self.feature_readiness_service.update_feature(
                    feature_id=feature_id,
                    readiness=FeatureReadiness.PRODUCTION_READY,
                    ignore_guardrails=bool(data.ignore_guardrails),
                    return_document=False,
                )

        return await self.get(document_id=feature_list_id)

    async def deploy_feature_list(
        self,
        feature_list_id: ObjectId,
        data: FeatureListUpdate,
    ) -> Optional[Task]:
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
            payload = FeatureListDeployTaskPayload(
                feature_list_id=feature_list_id,
                deployed=data.deployed,
                catalog_id=self.service.catalog_id,
            )

            task_id = await self.task_controller.task_manager.submit(payload=payload)
            return await self.task_controller.get_task(task_id=str(task_id))

    async def list_feature_lists(
        self,
        page: int = 1,
        page_size: int = 10,
        sort_by: str | None = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
        search: str | None = None,
        name: str | None = None,
        version: str | None = None,
        feature_list_namespace_id: ObjectId | None = None,
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
        search: str | None
            Search token to be used in filtering
        name: str | None
            Feature name to be used in filtering
        version: str | None
            Feature version to be used in filtering
        feature_list_namespace_id: ObjectId | None
            Feature list namespace ID to be used in filtering

        Returns
        -------
        FeatureListPaginatedList
            List of documents fulfilled the filtering condition
        """
        params: Dict[str, Any] = {"search": search, "name": name}
        if version:
            params["version"] = VersionIdentifier.from_str(version).dict()

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

    async def preview(
        self, featurelist_preview: FeatureListPreview, get_credential: Any
    ) -> dict[str, Any]:
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
        dict[str, Any]
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
        except (MissingPointInTimeColumnError, RequiredEntityNotProvidedError) as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=exc.args[0]
            ) from exc

    async def get_historical_features(
        self,
        observation_set: UploadFile,
        featurelist_get_historical_features: FeatureListGetHistoricalFeatures,
        get_credential: Any,
    ) -> StreamingResponse:
        """
        Get historical features for Feature List

        Parameters
        ----------
        observation_set: UploadFile
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
                observation_set=dataframe_from_arrow_stream(observation_set.file),
                featurelist_get_historical_features=featurelist_get_historical_features,
                get_credential=get_credential,
            )
        except (
            MissingPointInTimeColumnError,
            TooRecentPointInTimeError,
            RequiredEntityNotProvidedError,
            UnexpectedServingNamesMappingError,
        ) as exc:
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
        observation_set: UploadFile,
        featurelist_get_historical_features: FeatureListGetHistoricalFeatures,
    ) -> str:
        """
        Get historical features sql for Feature List

        Parameters
        ----------
        observation_set: UploadFile
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
                observation_set=dataframe_from_arrow_stream(observation_set.file),
                featurelist_get_historical_features=featurelist_get_historical_features,
            )
        except (MissingPointInTimeColumnError, TooRecentPointInTimeError) as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=exc.args[0]
            ) from exc

    async def get_online_features(
        self,
        feature_list_id: ObjectId,
        data: FeatureListGetOnlineFeatures,
        get_credential: Any,
    ) -> OnlineFeaturesResponseModel:
        """
        Get historical features for Feature List

        Parameters
        ----------
        feature_list_id: ObjectId
            Id of the Feature List
        data: FeatureListGetOnlineFeatures
            FeatureListGetHistoricalFeatures object
        get_credential: Any
            Get credential handler function

        Returns
        -------
        OnlineFeaturesResponseModel

        Raises
        ------
        HTTPException
            Invalid request payload
        """
        feature_list = await self.service.get_document(feature_list_id)
        try:
            result = await self.online_serving_service.get_online_features_from_feature_list(
                feature_list=feature_list,
                entity_serving_names=data.entity_serving_names,
                get_credential=get_credential,
            )
        except (FeatureListNotOnlineEnabledError, RuntimeError) as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=exc.args[0]
            ) from exc
        return result

    async def get_feature_job_logs(
        self, feature_list_id: ObjectId, hour_limit: int, get_credential: Any
    ) -> dict[str, Any]:
        """
        Retrieve data preview for query graph node

        Parameters
        ----------
        feature_list_id: ObjectId
            FeatureList Id
        hour_limit: int
            Limit in hours on the job history to fetch
        get_credential: Any
            Get credential handler function

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        feature_list = await self.service.get_document(feature_list_id)
        assert feature_list.feature_clusters
        assert len(feature_list.feature_clusters) == 1

        features = []
        async for doc in self.feature_service.list_documents_iterator(
            query_filter={"_id": {"$in": feature_list.feature_ids}}
        ):
            features.append(ExtendedFeatureModel(**doc))

        return await self.feature_store_warehouse_service.get_feature_job_logs(
            feature_store_id=feature_list.feature_clusters[0].feature_store_id,
            features=features,
            hour_limit=hour_limit,
            get_credential=get_credential,
        )
