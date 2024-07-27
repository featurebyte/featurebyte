"""
FeatureList API route controller
"""

from __future__ import annotations

import copy
from http import HTTPStatus
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set, Tuple, Union

from bson import ObjectId, json_util
from fastapi import UploadFile
from fastapi.exceptions import HTTPException

from featurebyte.common.utils import dataframe_from_arrow_stream
from featurebyte.exception import (
    DocumentDeletionError,
    MissingPointInTimeColumnError,
    RequiredEntityNotProvidedError,
    TooRecentPointInTimeError,
)
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.feature_list import FeatureListModel, FeatureReadinessDistribution
from featurebyte.models.persistent import QueryFilter
from featurebyte.persistent.base import SortDir
from featurebyte.routes.catalog.catalog_name_injector import CatalogNameInjector
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.feature_list import (
    FeatureListCreate,
    FeatureListCreateJob,
    FeatureListCreateWithBatchFeatureCreation,
    FeatureListGetHistoricalFeatures,
    FeatureListModelResponse,
    FeatureListNewVersionCreate,
    FeatureListPaginatedItem,
    FeatureListPaginatedList,
    FeatureListPreview,
    FeatureListServiceCreate,
    FeatureListSQL,
    FeatureListUpdate,
    SampleEntityServingNames,
)
from featurebyte.schema.info import (
    EntityBriefInfoList,
    FeatureListBriefInfoList,
    FeatureListInfo,
    TableBriefInfoList,
)
from featurebyte.schema.task import Task
from featurebyte.schema.worker.task.feature_list_batch_feature_create import (
    FeatureListCreateWithBatchFeatureCreationTaskPayload,
)
from featurebyte.schema.worker.task.feature_list_create import (
    FeatureListCreateTaskPayload,
    FeaturesParameters,
)
from featurebyte.schema.worker.task.feature_list_make_production_ready import (
    FeatureListMakeProductionReadyTaskPayload,
)
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_facade import FeatureListFacadeService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_preview import FeaturePreviewService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.mixin import DEFAULT_PAGE_SIZE
from featurebyte.service.table import TableService
from featurebyte.service.task_manager import TaskManager
from featurebyte.service.tile_job_log import TileJobLogService
from featurebyte.storage import Storage


class FeatureListController(
    BaseDocumentController[FeatureListModel, FeatureListService, FeatureListPaginatedList]
):
    """
    FeatureList controller
    """

    paginated_document_class = FeatureListPaginatedList

    def __init__(
        self,
        feature_list_service: FeatureListService,
        feature_list_facade_service: FeatureListFacadeService,
        feature_list_namespace_service: FeatureListNamespaceService,
        feature_service: FeatureService,
        entity_service: EntityService,
        table_service: TableService,
        deployment_service: DeploymentService,
        historical_feature_table_service: HistoricalFeatureTableService,
        catalog_name_injector: CatalogNameInjector,
        feature_preview_service: FeaturePreviewService,
        tile_job_log_service: TileJobLogService,
        task_controller: TaskController,
        task_manager: TaskManager,
        storage: Storage,
    ):
        super().__init__(feature_list_service)
        self.feature_list_facade_service = feature_list_facade_service
        self.feature_list_namespace_service = feature_list_namespace_service
        self.feature_service = feature_service
        self.entity_service = entity_service
        self.table_service = table_service
        self.deployment_service = deployment_service
        self.historical_feature_table_service = historical_feature_table_service
        self.catalog_name_injector = catalog_name_injector
        self.feature_preview_service = feature_preview_service
        self.tile_job_log_service = tile_job_log_service
        self.task_controller = task_controller
        self.task_manager = task_manager
        self.storage = storage

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
        payload = FeatureListCreateWithBatchFeatureCreationTaskPayload(**{
            **data.model_dump(by_alias=True),
            "user_id": self.service.user.id,
            "catalog_id": self.service.catalog_id,
            "output_document_id": data.id,
        })
        task_id = await self.task_manager.submit(payload=payload)
        return await self.task_manager.get_task(task_id=str(task_id))

    async def submit_feature_list_create_job(self, data: FeatureListCreateJob) -> Optional[Task]:
        """
        Submit feature list creation task

        Parameters
        ----------
        data: FeatureListCreateJob
            Feature list creation job payload

        Returns
        -------
        Optional[Task]
            Task object
        """

        features_parameters_path = self.service.get_full_remote_file_path(
            f"feature_list/{data.id}/features_parameters_{ObjectId()}.json"
        )
        feature_parameters = FeaturesParameters(features=data.features)
        await self.storage.put_text(
            json_util.dumps(feature_parameters.model_dump(by_alias=True)), features_parameters_path
        )
        payload = FeatureListCreateTaskPayload(**{
            "feature_list_id": data.id,
            "feature_list_name": data.name,
            "features_parameters_path": str(features_parameters_path),
            "features_conflict_resolution": data.features_conflict_resolution,
            "user_id": self.service.user.id,
            "catalog_id": self.service.catalog_id,
            "output_document_id": data.id,
        })
        task_id = await self.task_manager.submit(payload=payload)
        return await self.task_manager.get_task(task_id=str(task_id))

    async def create_feature_list(
        self,
        data: Union[FeatureListCreate, FeatureListNewVersionCreate],
        progress_callback: Optional[Callable[..., Coroutine[Any, Any, None]]] = None,
    ) -> FeatureListModelResponse:
        """
        Create FeatureList at persistent (GitDB or MongoDB)

        Parameters
        ----------
        data: FeatureListCreate | FeatureListNewVersionCreate
            Feature list creation payload
        progress_callback: Optional[Callable[..., Coroutine[Any, Any, None]]]
            Progress callback

        Returns
        -------
        FeatureListModelResponse
            Newly created feature list object
        """
        if isinstance(data, FeatureListCreate):
            create_data = FeatureListServiceCreate(**data.model_dump(by_alias=True))
            document = await self.feature_list_facade_service.create_feature_list(
                data=create_data, progress_callback=progress_callback
            )
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
            **document.model_dump(by_alias=True),
            is_default=namespace.default_feature_list_id == document.id,
        )
        return output

    async def update_feature_list(
        self,
        feature_list_id: ObjectId,
        data: FeatureListUpdate,
    ) -> Union[FeatureListModelResponse, Task]:
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
        Union[FeatureListModelResponse, Task]
            FeatureList object with updated attribute(s) or Task object

        """
        if data.make_production_ready:
            payload = FeatureListMakeProductionReadyTaskPayload(
                feature_list_id=feature_list_id,
                ignore_guardrails=bool(data.ignore_guardrails),
                user_id=self.service.user.id,
                catalog_id=self.service.catalog_id,
            )
            task_id = await self.task_manager.submit(payload=payload)
            return await self.task_controller.get_task(task_id=str(task_id))

        return await self.get(document_id=feature_list_id)

    async def delete_feature_list(self, feature_list_id: ObjectId) -> None:
        """
        Delete FeatureList at persistent

        Parameters
        ----------
        feature_list_id: ObjectId
            FeatureList ID
        """
        await self.verify_operation_by_checking_reference(
            document_id=feature_list_id, exception_class=DocumentDeletionError
        )
        await self.feature_list_facade_service.delete_feature_list(feature_list_id=feature_list_id)

    async def list_feature_lists(
        self,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_by: list[tuple[str, SortDir]] | None = None,
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
        sort_by: list[tuple[str, SortDir]] | None
            Keys and directions used to sort the returning documents
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
            params["version"] = VersionIdentifier.from_str(version).model_dump()

        if feature_list_namespace_id:
            query_filter = params.get("query_filter", {}).copy()
            query_filter["feature_list_namespace_id"] = feature_list_namespace_id
            params["query_filter"] = query_filter

        # list documents from persistent
        document_data = await self.service.list_documents_as_dict(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            projection={"feature_clusters": 0},  # exclude feature_clusters
            **params,
        )

        # prepare mappings to add additional attributes
        namespace_ids = {
            document["feature_list_namespace_id"] for document in document_data["data"]
        }
        namespace_id_to_default_id = {}
        async for namespace in self.feature_list_namespace_service.list_documents_as_dict_iterator(
            query_filter={"_id": {"$in": list(namespace_ids)}},
            projection={"_id": 1, "default_feature_list_id": 1},
        ):
            namespace_id_to_default_id[namespace["_id"]] = namespace["default_feature_list_id"]

        # prepare output
        output = []
        for feature_list in document_data["data"]:
            default_feature_list_id = namespace_id_to_default_id.get(
                feature_list["feature_list_namespace_id"]
            )
            output.append(
                FeatureListPaginatedItem(
                    **feature_list,
                    is_default=default_feature_list_id == feature_list["_id"],
                )
            )

        document_data["data"] = output
        return self.paginated_document_class(**document_data)

    async def preview(self, featurelist_preview: FeatureListPreview) -> dict[str, Any]:
        """
        Preview a Feature List

        Parameters
        ----------
        featurelist_preview: FeatureListPreview
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
            return await self.feature_preview_service.preview_featurelist(
                featurelist_preview=featurelist_preview
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

        def _to_version_str(version_dict: Optional[Dict[str, Any]]) -> Optional[str]:
            if not version_dict:
                return None
            return VersionIdentifier(**version_dict).to_str()

        def _to_prod_ready_fraction(readiness_dist: List[Dict[str, Any]]) -> float:
            return FeatureReadinessDistribution(readiness_dist).derive_production_ready_fraction()

        def _to_default_feature_fraction(
            feature_ids: List[ObjectId], default_feat_ids: Set[ObjectId]
        ) -> float:
            count = 0
            for feat_id in feature_ids:
                if feat_id in default_feat_ids:
                    count += 1
            return count / len(feature_ids)

        feature_list = await self.service.get_document_as_dict(document_id=document_id)
        namespace = await self.feature_list_namespace_service.get_document_as_dict(
            document_id=feature_list["feature_list_namespace_id"]
        )
        entities = await self.entity_service.list_documents_as_dict(
            page=1, page_size=0, query_filter={"_id": {"$in": feature_list["entity_ids"]}}
        )
        tables = await self.table_service.list_documents_as_dict(
            page=1, page_size=0, query_filter={"_id": {"$in": feature_list["table_ids"]}}
        )
        # get catalog info
        catalog_name, updated_docs = await self.catalog_name_injector.add_name(
            namespace["catalog_id"], [entities, tables]
        )
        entities, tables = updated_docs
        primary_entity_data = copy.deepcopy(entities)
        primary_entity_data["data"] = sorted(
            [
                entity
                for entity in entities["data"]
                if entity["_id"] in feature_list["primary_entity_ids"]
            ],
            key=lambda doc: doc["_id"],  # type: ignore
        )
        default_feature_list = await self.service.get_document_as_dict(
            document_id=namespace["default_feature_list_id"]
        )

        versions_info = None
        if verbose:
            versions_info = FeatureListBriefInfoList.from_paginated_data(
                await self.service.list_documents_as_dict(
                    page=1,
                    page_size=0,
                    query_filter={"_id": {"$in": namespace["feature_list_ids"]}},
                    projection={
                        "readiness_distribution": 1,
                        "version": 1,
                        "created_at": 1,
                    },
                )
            )

        default_feature_ids = set(default_feature_list["feature_ids"])
        return FeatureListInfo(
            version={
                "this": _to_version_str(feature_list["version"]),
                "default": _to_version_str(default_feature_list["version"]),
            },
            production_ready_fraction={
                "this": _to_prod_ready_fraction(feature_list["readiness_distribution"]),
                "default": _to_prod_ready_fraction(default_feature_list["readiness_distribution"]),
            },
            default_feature_fraction={
                "this": _to_default_feature_fraction(
                    feature_list["feature_ids"], default_feature_ids
                ),
                "default": _to_default_feature_fraction(
                    default_feature_list["feature_ids"], default_feature_ids
                ),
            },
            versions_info=versions_info,
            catalog_name=catalog_name,
            entities=EntityBriefInfoList.from_paginated_data(entities),
            primary_entity=EntityBriefInfoList.from_paginated_data(primary_entity_data),
            tables=TableBriefInfoList.from_paginated_data(tables),
            name=namespace["name"],
            namespace_description=namespace["description"],
            default_feature_list_id=namespace["default_feature_list_id"],
            version_count=len(namespace["feature_list_ids"]),
            dtype_distribution=feature_list["dtype_distribution"],
            deployed=feature_list["deployed"],
            description=feature_list["description"],
            status=namespace["status"],
            feature_count=len(feature_list["feature_ids"]),
            created_at=feature_list["created_at"],
        )

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

    async def get_sample_entity_serving_names(
        self, feature_list_id: ObjectId, count: int
    ) -> SampleEntityServingNames:
        """
        Get sample entity serving names for feature list

        Parameters
        ----------
        feature_list_id: ObjectId
            Feature list ID
        count: int
            Number of sample entity serving names to return

        Returns
        -------
        SampleEntityServingNames
            Sample entity serving names
        """

        entity_serving_names = await self.service.get_sample_entity_serving_names(
            feature_list_id=feature_list_id, count=count
        )
        return SampleEntityServingNames(entity_serving_names=entity_serving_names)

    async def service_and_query_pairs_for_checking_reference(
        self, document_id: ObjectId
    ) -> List[Tuple[Any, QueryFilter]]:
        return [
            (self.deployment_service, {"feature_list_id": document_id}),
            (self.historical_feature_table_service, {"feature_list_id": document_id}),
        ]
