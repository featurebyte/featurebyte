"""
FeatureList API route controller
"""
from __future__ import annotations

from typing import Any, Dict, Literal, Optional, Union

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import UploadFile
from fastapi.exceptions import HTTPException

from featurebyte.common.utils import dataframe_from_arrow_stream
from featurebyte.exception import (
    MissingPointInTimeColumnError,
    RequiredEntityNotProvidedError,
    TooRecentPointInTimeError,
)
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.feature_list import (
    FeatureListCreate,
    FeatureListCreateWithBatchFeatureCreation,
    FeatureListGetHistoricalFeatures,
    FeatureListModelResponse,
    FeatureListNewVersionCreate,
    FeatureListPaginatedList,
    FeatureListPreview,
    FeatureListServiceCreate,
    FeatureListSQL,
    FeatureListUpdate,
)
from featurebyte.schema.info import FeatureListInfo
from featurebyte.schema.task import Task
from featurebyte.schema.worker.task.feature_list_batch_feature_create import (
    FeatureListCreateWithBatchFeatureCreationTaskPayload,
)
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_facade import FeatureListFacadeService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_preview import FeaturePreviewService
from featurebyte.service.mixin import DEFAULT_PAGE_SIZE
from featurebyte.service.tile_job_log import TileJobLogService


# pylint: disable=too-many-instance-attributes
class FeatureListController(
    BaseDocumentController[FeatureListModel, FeatureListService, FeatureListPaginatedList]
):
    """
    FeatureList controller
    """

    paginated_document_class = FeatureListPaginatedList

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        feature_list_service: FeatureListService,
        feature_list_facade_service: FeatureListFacadeService,
        feature_list_namespace_service: FeatureListNamespaceService,
        feature_service: FeatureService,
        feature_preview_service: FeaturePreviewService,
        tile_job_log_service: TileJobLogService,
        task_controller: TaskController,
    ):
        super().__init__(feature_list_service)
        self.feature_list_facade_service = feature_list_facade_service
        self.feature_list_namespace_service = feature_list_namespace_service
        self.feature_service = feature_service
        self.feature_preview_service = feature_preview_service
        self.tile_job_log_service = tile_job_log_service
        self.task_controller = task_controller

    async def submit_feature_list_create_with_batch_feature_create_task(
        self, data: FeatureListCreateWithBatchFeatureCreation
    ) -> Optional[Task]:
        """
        Submit feature list creation with batch feature creation task

        Parameters
        ----------
        data: FeatureListCreateWithBatchFeatureCreation
            Feature list creation with batch feature creation payload

        Returns
        -------
        Optional[Task]
            Task object
        """
        payload = FeatureListCreateWithBatchFeatureCreationTaskPayload(
            **{
                **data.dict(by_alias=True),
                "user_id": self.service.user.id,
                "catalog_id": self.service.catalog_id,
            }
        )
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.task_manager.get_task(task_id=str(task_id))

    async def create_feature_list(
        self, data: Union[FeatureListCreate, FeatureListNewVersionCreate]
    ) -> FeatureListModelResponse:
        """
        Create FeatureList at persistent (GitDB or MongoDB)

        Parameters
        ----------
        data: FeatureListCreate | FeatureListNewVersionCreate
            Feature list creation payload

        Returns
        -------
        FeatureListModelResponse
            Newly created feature list object
        """
        if isinstance(data, FeatureListCreate):
            create_data = FeatureListServiceCreate(**data.dict(by_alias=True))
            document = await self.feature_list_facade_service.create_feature_list(data=create_data)
        else:
            document = await self.feature_list_facade_service.create_new_version(data=data)
        return await self.get(document_id=document.id)

    async def get(
        self, document_id: ObjectId, exception_detail: str | None = None
    ) -> FeatureListModelResponse:
        document = await self.service.get_document(
            document_id=document_id,
            exception_detail=exception_detail,
        )
        namespace = await self.feature_list_namespace_service.get_document(
            document_id=document.feature_list_namespace_id
        )
        output = FeatureListModelResponse(
            **document.dict(by_alias=True),
            is_default=namespace.default_feature_list_id == document.id,
        )
        return output

    async def update_feature_list(
        self,
        feature_list_id: ObjectId,
        data: FeatureListUpdate,
    ) -> FeatureListModelResponse:
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
        FeatureListModelResponse
            FeatureList object with updated attribute(s)
        """
        if data.make_production_ready:
            await self.feature_list_facade_service.make_features_production_ready(
                feature_list_id=feature_list_id,
                ignore_guardrails=bool(data.ignore_guardrails),
            )

        return await self.get(document_id=feature_list_id)

    async def delete_feature_list(self, feature_list_id: ObjectId) -> None:
        """
        Delete FeatureList at persistent

        Parameters
        ----------
        feature_list_id: ObjectId
            FeatureList ID
        """
        await self.feature_list_facade_service.delete_feature_list(feature_list_id=feature_list_id)

    async def list_feature_lists(
        self,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
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
        # pylint: disable=too-many-locals
        params: Dict[str, Any] = {"search": search, "name": name}
        if version:
            params["version"] = VersionIdentifier.from_str(version).dict()

        if feature_list_namespace_id:
            query_filter = params.get("query_filter", {}).copy()
            query_filter["feature_list_namespace_id"] = feature_list_namespace_id
            params["query_filter"] = query_filter

        # list documents from persistent
        document_data = await self.service.list_documents_as_dict(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            **params,
        )

        # prepare mappings to add additional attributes
        namespace_ids = {
            document["feature_list_namespace_id"] for document in document_data["data"]
        }
        namespace_id_to_default_id = {}
        async for namespace in self.feature_list_namespace_service.list_documents_as_dict_iterator(
            query_filter={"_id": {"$in": list(namespace_ids)}}
        ):
            namespace_id_to_default_id[namespace["_id"]] = namespace["default_feature_list_id"]

        # prepare output
        output = []
        for feature_list in document_data["data"]:
            default_feature_list_id = namespace_id_to_default_id.get(
                feature_list["feature_list_namespace_id"]
            )
            output.append(
                FeatureListModelResponse(
                    **feature_list,
                    is_default=default_feature_list_id == feature_list["_id"],
                )
            )

        document_data["data"] = output
        return self.paginated_document_class(**document_data)

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
            return await self.feature_preview_service.preview_featurelist(
                featurelist_preview=featurelist_preview, get_credential=get_credential
            )
        except (MissingPointInTimeColumnError, RequiredEntityNotProvidedError) as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=exc.args[0]
            ) from exc

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
        info_document = await self.service.get_feature_list_info(
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
        return await self.feature_preview_service.featurelist_sql(featurelist_sql=featurelist_sql)

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
            return await self.feature_preview_service.get_historical_features_sql(
                observation_set=dataframe_from_arrow_stream(observation_set.file),
                featurelist_get_historical_features=featurelist_get_historical_features,
            )
        except (MissingPointInTimeColumnError, TooRecentPointInTimeError) as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=exc.args[0]
            ) from exc

    async def get_feature_job_logs(
        self, feature_list_id: ObjectId, hour_limit: int
    ) -> dict[str, Any]:
        """
        Retrieve data preview for query graph node

        Parameters
        ----------
        feature_list_id: ObjectId
            FeatureList Id
        hour_limit: int
            Limit in hours on the job history to fetch

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        feature_list = await self.service.get_document(feature_list_id)
        assert feature_list.feature_clusters
        assert len(feature_list.feature_clusters) == 1

        features = []
        async for doc in self.feature_service.list_documents_as_dict_iterator(
            query_filter={"_id": {"$in": feature_list.feature_ids}}
        ):
            features.append(ExtendedFeatureModel(**doc))

        return await self.tile_job_log_service.get_feature_job_logs(
            features=features,
            hour_limit=hour_limit,
        )
