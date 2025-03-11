"""
Deployment API route controller
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Any, List, Literal, Optional, Tuple

from bson import ObjectId
from fastapi import HTTPException

from featurebyte.exception import (
    DeploymentNotEnabledError,
    DeploymentNotOnlineEnabledError,
    DocumentCreationError,
    DocumentDeletionError,
)
from featurebyte.feast.service.feature_store import FeastFeatureStoreService
from featurebyte.models.deployment import DeploymentModel
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.models.persistent import QueryFilter
from featurebyte.persistent.base import SortDir
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.deployment import (
    AllDeploymentList,
    AllDeploymentListRecord,
    DeploymentCreate,
    DeploymentJobHistory,
    DeploymentList,
    DeploymentSummary,
    DeploymentUpdate,
    OnlineFeaturesResponseModel,
)
from featurebyte.schema.feature_list import OnlineFeaturesRequestPayload, SampleEntityServingNames
from featurebyte.schema.info import DeploymentInfo, DeploymentRequestCodeTemplate
from featurebyte.schema.task import Task
from featurebyte.schema.worker.task.deployment_create_update import (
    CreateDeploymentPayload,
    DeploymentCreateUpdateTaskPayload,
    UpdateDeploymentPayload,
)
from featurebyte.service.batch_feature_table import BatchFeatureTableService
from featurebyte.service.catalog import AllCatalogService, CatalogService
from featurebyte.service.context import ContextService
from featurebyte.service.deployment import AllDeploymentService, DeploymentService
from featurebyte.service.entity_serving_names import EntityServingNamesService
from featurebyte.service.feature_job_history_service import FeatureJobHistoryService
from featurebyte.service.feature_list import AllFeatureListService, FeatureListService
from featurebyte.service.mixin import DEFAULT_PAGE_SIZE
from featurebyte.service.online_serving import OnlineServingService
from featurebyte.service.use_case import UseCaseService


class DeploymentController(
    BaseDocumentController[DeploymentModel, DeploymentService, DeploymentList]
):
    """
    Deployment Controller
    """

    paginated_document_class = DeploymentList

    def __init__(
        self,
        deployment_service: DeploymentService,
        catalog_service: CatalogService,
        context_service: ContextService,
        feature_list_service: FeatureListService,
        online_serving_service: OnlineServingService,
        task_controller: TaskController,
        use_case_service: UseCaseService,
        batch_feature_table_service: BatchFeatureTableService,
        feast_feature_store_service: FeastFeatureStoreService,
        entity_serving_names_service: EntityServingNamesService,
        feature_job_history_service: FeatureJobHistoryService,
    ):
        super().__init__(deployment_service)
        self.catalog_service = catalog_service
        self.context_service = context_service
        self.feature_list_service = feature_list_service
        self.online_serving_service = online_serving_service
        self.task_controller = task_controller
        self.use_case_service = use_case_service
        self.batch_feature_table_service = batch_feature_table_service
        self.feast_feature_store_service = feast_feature_store_service
        self.entity_serving_names_service = entity_serving_names_service
        self.feature_job_history_service = feature_job_history_service

    async def create_deployment(self, data: DeploymentCreate) -> Task:
        """
        Create deployment.

        Parameters
        ----------
        data : DeploymentCreate
            Deployment data to create.

        Returns
        -------
        Task
            Task to create deployment.

        Raises
        ------
        DocumentCreationError
            Primary entity of the use case is not in the feature list's supported serving entities.
        """
        # check if feature list exists
        feature_list_doc = await self.feature_list_service.get_document_as_dict(
            document_id=data.feature_list_id
        )

        context_id = None
        if data.use_case_id:
            use_case = await self.use_case_service.get_document(document_id=data.use_case_id)
            context_id = use_case.context_id
            context = await self.context_service.get_document(document_id=context_id)

            # check whether the context primary entity is in the feature list supported serving entities
            supported_serving_ids = feature_list_doc["supported_serving_entity_ids"]
            if supported_serving_ids and context.primary_entity_ids not in supported_serving_ids:
                raise DocumentCreationError(
                    "Primary entity of the use case is not in the feature list's supported serving entities."
                )

        payload = DeploymentCreateUpdateTaskPayload(
            deployment_payload=CreateDeploymentPayload(
                name=data.name,
                feature_list_id=data.feature_list_id,
                enabled=False,
                use_case_id=data.use_case_id,
                context_id=context_id,
            ),
            user_id=self.service.user.id,
            catalog_id=self.service.catalog_id,
            output_document_id=data.id or ObjectId(),
        )
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))

    async def update_deployment(
        self, document_id: ObjectId, data: DeploymentUpdate
    ) -> Optional[Task]:
        """
        Update deployment.

        Parameters
        ----------
        document_id: ObjectId
            Deployment ID to update.
        data: DeploymentUpdate
            Deployment data to update.

        Returns
        -------
        Optional[Task]
            Task to create deployment.
        """
        # check if deployment exists
        deployment = await self.service.get_document(document_id=document_id)
        if data.enabled is not None and data.enabled != deployment.enabled:
            payload = DeploymentCreateUpdateTaskPayload(
                deployment_payload=UpdateDeploymentPayload(enabled=data.enabled),
                user_id=self.service.user.id,
                catalog_id=self.service.catalog_id,
                output_document_id=document_id,
            )
            task_id = await self.task_controller.task_manager.submit(payload=payload)
            return await self.task_controller.get_task(task_id=str(task_id))
        return None

    async def service_and_query_pairs_for_checking_reference(
        self, document_id: ObjectId
    ) -> List[Tuple[Any, QueryFilter]]:
        return [(self.batch_feature_table_service, {"deployment_id": document_id})]

    async def delete(self, document_id: ObjectId) -> None:
        deployment = await self.service.get_document(document_id=document_id)
        if deployment.enabled:
            raise DocumentDeletionError("Only disabled deployment can be deleted.")
        await super().delete(document_id=document_id)

    async def get_info(self, document_id: ObjectId, verbose: bool) -> DeploymentInfo:
        """
        Get deployment info.

        Parameters
        ----------
        document_id: ObjectId
            Deployment ID to get info
        verbose: bool
            Whether to return verbose info

        Returns
        -------
        DeploymentInfo
        """
        _ = verbose
        deployment = await self.service.get_document(document_id=document_id)
        feature_list = await self.feature_list_service.get_document(
            document_id=deployment.feature_list_id
        )
        use_case_name = None
        if deployment.use_case_id:
            use_case = await self.use_case_service.get_document(document_id=deployment.use_case_id)
            use_case_name = use_case.name

        return DeploymentInfo(
            name=deployment.name,
            feature_list_name=feature_list.name,
            feature_list_version=feature_list.version.to_str(),
            num_feature=len(feature_list.feature_ids),
            enabled=deployment.enabled,
            serving_endpoint=(
                f"/deployment/{deployment.id}/online_features" if deployment.enabled else None
            ),
            created_at=deployment.created_at,
            updated_at=deployment.updated_at,
            description=deployment.description,
            use_case_name=use_case_name,
        )

    async def compute_online_features(
        self,
        deployment_id: ObjectId,
        data: OnlineFeaturesRequestPayload,
    ) -> OnlineFeaturesResponseModel:
        """
        Compute online features for a given deployment ID.

        Parameters
        ----------
        deployment_id: ObjectId
            ID of deployment to compute online features
        data: OnlineFeaturesRequestPayload
            Online features request payload

        Returns
        -------
        OnlineFeaturesResponseModel

        Raises
        ------
        HTTPException
            Invalid request payload
        """
        deployment = await self.service.get_document(deployment_id)

        # deployment must be online enabled
        await self._validate_deployment_is_online_enabled(deployment)

        feature_list = await self.feature_list_service.get_document(deployment.feature_list_id)
        catalog = await self.catalog_service.get_document(feature_list.catalog_id)
        try:
            result: Optional[OnlineFeaturesResponseModel]
            if feature_list.feast_enabled and catalog.online_store_id is not None:
                feast_store = (
                    await self.feast_feature_store_service.get_feast_feature_store_for_deployment(
                        deployment=deployment
                    )
                )
            else:
                feast_store = None

            feast_feature_services = set()
            if feast_store is not None:
                feast_feature_services.update([
                    fs.name for fs in feast_store.list_feature_services()
                ])

            if feast_store and feature_list.versioned_name in feast_feature_services:
                result = await self.online_serving_service.get_online_features_by_feast(
                    feature_list=feature_list,
                    deployment=deployment,
                    feast_store=feast_store,
                    request_data=data.entity_serving_names,
                )
            else:
                result = await self.online_serving_service.get_online_features_from_feature_list(
                    feature_list=feature_list,
                    request_data=data.entity_serving_names,
                )
        except (DeploymentNotEnabledError, RuntimeError) as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=exc.args[0]
            ) from exc
        assert result is not None, result
        return result

    async def _validate_deployment_is_online_enabled(self, deployment: DeploymentModel) -> None:
        """
        Validate deployment is online enabled.

        Parameters
        ----------
        deployment: DeploymentModel
            Deployment to validate

        Raises
        ------
        DeploymentNotOnlineEnabledError
            Deployment is not online enabled
        """
        catalog = await self.catalog_service.get_document(document_id=deployment.catalog_id)
        if catalog.online_store_id is None:
            raise DeploymentNotOnlineEnabledError(
                "Deployment is not online enabled. Configure online store for the catalog to enable online feature requests."
            )

    async def get_request_code_template(
        self, deployment_id: ObjectId, language: Literal["python", "sh"]
    ) -> DeploymentRequestCodeTemplate:
        """
        Get request code template for a given deployment ID.

        Parameters
        ----------
        deployment_id: ObjectId
            ID of deployment to get request code template
        language: Literal["python", "sh"]
            Language of request code template

        Returns
        -------
        DeploymentRequestCodeTemplate
            Request code template

        Raises
        ------
        DeploymentNotEnabledError
            Feature list is not online enabled
        """
        deployment: DeploymentModel = await self.service.get_document(deployment_id)
        if not deployment.enabled:
            raise DeploymentNotEnabledError("Deployment is not enabled.")

        # deployment must be online enabled
        await self._validate_deployment_is_online_enabled(deployment)

        return await self.online_serving_service.get_request_code_template(
            deployment=deployment,
            language=language,
        )

    async def get_sample_entity_serving_names(
        self, deployment_id: ObjectId, count: int
    ) -> SampleEntityServingNames:
        """
        Get request code template for a given deployment ID.

        Parameters
        ----------
        deployment_id: ObjectId
            ID of deployment to get request code template
        count: int
            Number of sample entity serving names to return

        Returns
        -------
        SampleEntityServingNames
            Sample entity serving names
        """
        deployment: DeploymentModel = await self.service.get_document(deployment_id)
        entity_serving_names = []
        if deployment.serving_entity_ids:
            entity_serving_names = (
                await self.entity_serving_names_service.get_sample_entity_serving_names(
                    entity_ids=deployment.serving_entity_ids, table_ids=None, count=count
                )
            )
        return SampleEntityServingNames(entity_serving_names=entity_serving_names)

    async def get_deployment_job_history(
        self, deployment_id: ObjectId, num_runs: int
    ) -> DeploymentJobHistory:
        """
        Get job history for a deployment

        Parameters
        ----------
        deployment_id: ObjectId
            Deployment ID
        num_runs: int
            Number of runs to retrieve

        Returns
        -------
        DeploymentJobHistory
        """
        return await self.feature_job_history_service.get_deployment_job_history(
            deployment_id=deployment_id,
            num_runs=num_runs,
        )


class AllDeploymentController(
    BaseDocumentController[DeploymentModel, AllDeploymentService, DeploymentList]
):
    """
    All Deployment Controller
    """

    paginated_document_class = DeploymentList

    def __init__(
        self,
        all_deployment_service: AllDeploymentService,
        all_catalog_service: AllCatalogService,
        all_feature_list_service: AllFeatureListService,
        task_controller: TaskController,
    ):
        super().__init__(all_deployment_service)
        self.catalog_service = all_catalog_service
        self.feature_list_service = all_feature_list_service
        self.task_controller = task_controller

    async def get_deployment_summary(self) -> DeploymentSummary:
        """
        Get summary of all deployments.

        Returns
        -------
        DeploymentSummary
            Summary of all deployments.
        """
        feature_list_ids = set()
        feature_ids = set()
        with self.service.allow_use_raw_query_filter():
            deployment_data = await self.service.list_documents_as_dict(
                page=1,
                page_size=0,
                query_filter={"enabled": True},
                use_raw_query_filter=True,
            )

        for doc in deployment_data["data"]:
            deployment_model = DeploymentModel(**doc)
            feature_list_ids.add(deployment_model.feature_list_id)

        with self.feature_list_service.allow_use_raw_query_filter():
            async for feature_list in self.feature_list_service.list_documents_as_dict_iterator(
                query_filter={"_id": {"$in": list(feature_list_ids)}},
                projection={"feature_ids": 1},
                use_raw_query_filter=True,
            ):
                feature_ids.update(set(feature_list["feature_ids"]))

        return DeploymentSummary(
            num_feature_list=len(feature_list_ids),
            num_feature=len(feature_ids),
        )

    async def list_all_deployments(
        self,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_by: list[tuple[str, SortDir]] | None = None,
        enabled: bool | None = None,
    ) -> AllDeploymentList:
        """
        List all deployments across all catalogs.

        Parameters
        ----------
        page: int
            Page number
        page_size: int
            Number of items per page
        sort_by: list[tuple[str, SortDir]] | None
            Keys and directions used to sort the returning documents
        enabled: bool | None
            Whether to return only enabled deployments

        Returns
        -------
        AllDeploymentList
        """
        with self.service.allow_use_raw_query_filter():
            deployment_data = await self.service.list_documents_as_dict(
                page=page,
                page_size=page_size,
                sort_by=sort_by,
                query_filter={"enabled": enabled} if enabled is not None else {},
                use_raw_query_filter=True,
            )

        feature_list_ids = {doc["feature_list_id"] for doc in deployment_data["data"]}
        with self.feature_list_service.allow_use_raw_query_filter():
            feature_list_documents = await self.feature_list_service.list_documents_as_dict(
                page_size=0,
                query_filter={"_id": {"$in": list(feature_list_ids)}},
                projection={"feature_clusters": 0},  # exclude feature clusters
                use_raw_query_filter=True,
            )
            deployment_id_to_feature_list = {
                doc["_id"]: FeatureListModel(**doc) for doc in feature_list_documents["data"]
            }

        catalog_ids = {doc["catalog_id"] for doc in deployment_data["data"]}
        catalog_documents = await self.catalog_service.list_documents_as_dict(
            page_size=0, query_filter={"_id": {"$in": list(catalog_ids)}}
        )
        deployment_id_to_catalog_name = {
            doc["_id"]: doc["name"] for doc in catalog_documents["data"]
        }

        output = []
        for doc in deployment_data["data"]:
            feature_list = deployment_id_to_feature_list[doc["feature_list_id"]]
            output.append(
                AllDeploymentListRecord(
                    **doc,
                    catalog_name=deployment_id_to_catalog_name[doc["catalog_id"]],
                    feature_list_name=feature_list.name,
                    feature_list_version=feature_list.version.to_str(),
                    num_feature=len(feature_list.feature_ids),
                )
            )

        deployment_data["data"] = output
        return AllDeploymentList(**deployment_data)
