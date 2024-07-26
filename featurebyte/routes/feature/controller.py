"""
Feature API route controller
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Any, Dict, Optional, Union

from bson import ObjectId
from fastapi.exceptions import HTTPException

from featurebyte.exception import MissingPointInTimeColumnError, RequiredEntityNotProvidedError
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_namespace import FeatureReadiness
from featurebyte.persistent.base import SortDir
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.common.feature_metadata_extractor import FeatureOrTargetMetadataExtractor
from featurebyte.routes.feature_namespace.controller import FeatureNamespaceController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.feature import (
    BatchFeatureCreate,
    FeatureBriefInfoList,
    FeatureCreate,
    FeatureModelResponse,
    FeatureNewVersionCreate,
    FeaturePaginatedList,
    FeatureServiceCreate,
    FeatureSQL,
    FeatureUpdate,
)
from featurebyte.schema.feature_list import SampleEntityServingNames
from featurebyte.schema.info import FeatureInfo
from featurebyte.schema.preview import FeaturePreview
from featurebyte.schema.task import Task
from featurebyte.schema.worker.task.batch_feature_create import BatchFeatureCreateTaskPayload
from featurebyte.service.catalog import CatalogService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_facade import FeatureFacadeService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_preview import FeaturePreviewService
from featurebyte.service.table import TableService
from featurebyte.service.tile_job_log import TileJobLogService


class FeatureController(
    BaseDocumentController[FeatureModelResponse, FeatureService, FeaturePaginatedList]
):
    """
    Feature controller
    """

    paginated_document_class = FeaturePaginatedList

    def __init__(
        self,
        feature_service: FeatureService,
        feature_facade_service: FeatureFacadeService,
        feature_namespace_service: FeatureNamespaceService,
        feature_list_service: FeatureListService,
        feature_preview_service: FeaturePreviewService,
        task_controller: TaskController,
        catalog_service: CatalogService,
        table_service: TableService,
        feature_namespace_controller: FeatureNamespaceController,
        feature_or_target_metadata_extractor: FeatureOrTargetMetadataExtractor,
        tile_job_log_service: TileJobLogService,
    ):
        super().__init__(feature_service)
        self.feature_facade_service = feature_facade_service
        self.feature_namespace_service = feature_namespace_service
        self.feature_list_service = feature_list_service
        self.feature_preview_service = feature_preview_service
        self.task_controller = task_controller
        self.catalog_service = catalog_service
        self.table_service = table_service
        self.feature_namespace_controller = feature_namespace_controller
        self.feature_or_target_metadata_extractor = feature_or_target_metadata_extractor
        self.tile_job_log_service = tile_job_log_service

    async def submit_batch_feature_create_task(self, data: BatchFeatureCreate) -> Optional[Task]:
        """
        Submit Feature Create Task

        Parameters
        ----------
        data: BatchFeatureCreate
            Batch Feature creation payload

        Returns
        -------
        Optional[Task]
            Task object
        """
        # as there is no direct way to get the conflict resolved feature id for batch feature creation task,
        # the conflict resolution should only support "raise" for public API. Therefore, we should not include
        # the conflict resolution in the API payload schema (BatchFeatureCreate).
        payload = BatchFeatureCreateTaskPayload(**{
            **data.model_dump(by_alias=True),
            "user_id": self.service.user.id,
            "catalog_id": self.service.catalog_id,
        })
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.task_manager.get_task(task_id=str(task_id))

    async def create_feature(
        self, data: Union[FeatureCreate, FeatureNewVersionCreate]
    ) -> FeatureModelResponse:
        """
        Create Feature at persistent (GitDB or MongoDB)

        Parameters
        ----------
        data: FeatureCreate | FeatureNewVersionCreate
            Feature creation payload

        Returns
        -------
        FeatureModelResponse
            Newly created feature object
        """
        if isinstance(data, FeatureCreate):
            create_data = FeatureServiceCreate(**data.model_dump(by_alias=True))
            document = await self.feature_facade_service.create_feature(data=create_data)
        else:
            document = await self.feature_facade_service.create_new_version(data=data)
        return await self.get(document_id=document.id)

    async def get(
        self, document_id: ObjectId, exception_detail: str | None = None
    ) -> FeatureModelResponse:
        document = await self.service.get_document(
            document_id=document_id,
            exception_detail=exception_detail,
        )
        namespace = await self.feature_namespace_service.get_document(
            document_id=document.feature_namespace_id
        )
        output = FeatureModelResponse(
            **document.model_dump(by_alias=True),
            is_default=namespace.default_feature_id == document.id,
        )
        return output

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
            await self.feature_facade_service.update_readiness(
                feature_id=feature_id,
                readiness=FeatureReadiness(data.readiness),
                ignore_guardrails=bool(data.ignore_guardrails),
            )
        return await self.get(document_id=feature_id)

    async def delete_feature(self, feature_id: ObjectId) -> None:
        """
        Delete Feature at persistent

        Parameters
        ----------
        feature_id: ObjectId
            Feature ID
        """
        await self.feature_facade_service.delete_feature(feature_id=feature_id)

    async def list_features(
        self,
        page: int = 1,
        page_size: int = 10,
        sort_by: list[tuple[str, SortDir]] | None = None,
        search: str | None = None,
        name: str | None = None,
        version: str | None = None,
        feature_list_id: ObjectId | None = None,
        feature_namespace_id: ObjectId | None = None,
    ) -> FeaturePaginatedList:
        """
        List documents stored at persistent (GitDB or MongoDB)

        Parameters
        ----------
        page: int
            Page number
        page_size: int
            Number of items per page
        sort_by: list[tuple[str, SortDir]] | None
            Keys and directions used to sort the returning documents
        search: str | None
            Search token to be used in filtering
        name: str | None
            Feature name to be used in filtering
        version: str | None
            Feature version to be used in filtering
        feature_list_id: ObjectId | None
            Feature list ID to be used in filtering
        feature_namespace_id: ObjectId | None
            Feature namespace ID to be used in filtering

        Returns
        -------
        FeaturePaginatedList
            List of documents fulfilled the filtering condition
        """

        params: Dict[str, Any] = {"search": search, "name": name}
        if version:
            params["version"] = VersionIdentifier.from_str(version).model_dump()

        if feature_list_id:
            feature_list_document = await self.feature_list_service.get_document(
                document_id=feature_list_id
            )
            params["query_filter"] = {"_id": {"$in": feature_list_document.feature_ids}}

        if feature_namespace_id:
            query_filter = params.get("query_filter", {}).copy()
            query_filter["feature_namespace_id"] = feature_namespace_id
            params["query_filter"] = query_filter

        # list documents from persistent
        document_data = await self.service.list_documents_as_dict(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            **params,
        )

        # prepare mappings to add additional attributes
        namespace_ids = {document["feature_namespace_id"] for document in document_data["data"]}
        namespace_id_to_default_id = {}
        async for namespace in self.feature_namespace_service.list_documents_as_dict_iterator(
            query_filter={"_id": {"$in": list(namespace_ids)}},
            projection={"_id": 1, "default_feature_id": 1},
        ):
            namespace_id_to_default_id[namespace["_id"]] = namespace["default_feature_id"]

        # prepare output
        output = []
        for feature in document_data["data"]:
            default_feature_id = namespace_id_to_default_id.get(feature["feature_namespace_id"])
            output.append(
                FeatureModelResponse(
                    **feature,
                    is_default=default_feature_id == feature["_id"],
                )
            )

        document_data["data"] = output
        return self.paginated_document_class(**document_data)

    async def preview(self, feature_preview: FeaturePreview) -> dict[str, Any]:
        """
        Preview a Feature

        Parameters
        ----------
        feature_preview: FeaturePreview
            FeaturePreview object

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
            return await self.feature_preview_service.preview_feature(
                feature_preview=feature_preview
            )
        except (MissingPointInTimeColumnError, RequiredEntityNotProvidedError) as exc:
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
        feature = await self.service.get_document(document_id=document_id)
        data_id_to_name = {
            doc["_id"]: doc["name"]
            async for doc in self.table_service.list_documents_as_dict_iterator(
                query_filter={"_id": {"$in": feature.table_ids}},
                projection={"_id": 1, "name": 1},
            )
        }
        namespace_info = await self.feature_namespace_controller.get_info(
            document_id=feature.feature_namespace_id,
            verbose=verbose,
        )
        default_feature = await self.service.get_document(
            document_id=namespace_info.default_feature_id
        )
        versions_info = None
        if verbose:
            namespace = await self.feature_namespace_service.get_document(
                document_id=feature.feature_namespace_id
            )
            versions_info = FeatureBriefInfoList.from_paginated_data(
                await self.service.list_documents_as_dict(
                    page=1,
                    page_size=0,
                    query_filter={"_id": {"$in": namespace.feature_ids}},
                )
            )

        _, metadata = await self.feature_or_target_metadata_extractor.extract_from_object(feature)

        namespace_info_dict = namespace_info.model_dump()
        # use feature list description instead of namespace description
        namespace_description = namespace_info_dict.pop("description", None)
        return FeatureInfo(
            **namespace_info_dict,
            version={"this": feature.version.to_str(), "default": default_feature.version.to_str()},
            readiness={"this": feature.readiness, "default": default_feature.readiness},
            table_feature_job_setting={
                "this": feature.extract_table_feature_job_settings(
                    table_id_to_name=data_id_to_name, keep_first_only=True
                ),
                "default": default_feature.extract_table_feature_job_settings(
                    table_id_to_name=data_id_to_name, keep_first_only=True
                ),
            },
            table_cleaning_operation={
                "this": feature.extract_table_cleaning_operations(
                    table_id_to_name=data_id_to_name, keep_all_columns=False
                ),
                "default": default_feature.extract_table_cleaning_operations(
                    table_id_to_name=data_id_to_name, keep_all_columns=False
                ),
            },
            versions_info=versions_info,
            metadata=metadata,
            namespace_description=namespace_description,
            description=feature.description,
        )

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
        return await self.feature_preview_service.feature_sql(feature_sql=feature_sql)

    async def get_feature_job_logs(self, feature_id: ObjectId, hour_limit: int) -> dict[str, Any]:
        """
        Retrieve table preview for query graph node

        Parameters
        ----------
        feature_id: ObjectId
            Feature Id
        hour_limit: int
            Limit in hours on the job history to fetch

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        feature = await self.service.get_document(feature_id)
        return await self.tile_job_log_service.get_feature_job_logs(
            features=[ExtendedFeatureModel(**feature.model_dump(by_alias=True))],
            hour_limit=hour_limit,
        )

    async def get_sample_entity_serving_names(
        self, feature_id: ObjectId, count: int
    ) -> SampleEntityServingNames:
        """
        Get sample entity serving names for feature

        Parameters
        ----------
        feature_id: ObjectId
            Feature ID
        count: int
            Number of sample entity serving names to return

        Returns
        -------
        SampleEntityServingNames
            Sample entity serving names
        """

        entity_serving_names = await self.service.get_sample_entity_serving_names(
            feature_id=feature_id, count=count
        )
        return SampleEntityServingNames(entity_serving_names=entity_serving_names)
