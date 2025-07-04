"""
BatchFeatureTable API route controller
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional, Union

from bson import ObjectId

from featurebyte.exception import FeatureTableRequestInputNotFoundError
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.routes.common.base_materialized_table import BaseMaterializedTableController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.batch_feature_table import (
    BatchExternalFeatureTableCreate,
    BatchFeatureTableCreate,
    BatchFeatureTableList,
)
from featurebyte.schema.info import BatchFeatureTableInfo
from featurebyte.schema.task import Task
from featurebyte.service.batch_external_feature_table import BatchExternalFeatureTableService
from featurebyte.service.batch_feature_table import BatchFeatureTableService
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.catalog import CatalogService
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService


class BatchFeatureTableController(
    BaseMaterializedTableController[
        BatchFeatureTableModel, BatchFeatureTableService, BatchFeatureTableList
    ],
):
    """
    BatchFeatureTable Controller
    """

    paginated_document_class = BatchFeatureTableList
    has_internal_row_index_column_in_table = False

    def __init__(
        self,
        batch_feature_table_service: BatchFeatureTableService,
        batch_external_feature_table_service: BatchExternalFeatureTableService,
        catalog_service: CatalogService,
        feature_store_warehouse_service: FeatureStoreWarehouseService,
        feature_store_service: FeatureStoreService,
        feature_list_service: FeatureListService,
        feature_service: FeatureService,
        batch_request_table_service: BatchRequestTableService,
        deployment_service: DeploymentService,
        entity_validation_service: EntityValidationService,
        task_controller: TaskController,
    ):
        super().__init__(
            service=batch_feature_table_service,
            feature_store_warehouse_service=feature_store_warehouse_service,
        )
        self.catalog_service = catalog_service
        self.feature_store_service = feature_store_service
        self.batch_external_feature_table_service = batch_external_feature_table_service
        self.feature_list_service = feature_list_service
        self.feature_service = feature_service
        self.batch_request_table_service = batch_request_table_service
        self.deployment_service = deployment_service
        self.entity_validation_service = entity_validation_service
        self.task_controller = task_controller

    async def create_batch_feature_table(
        self,
        data: Union[BatchFeatureTableCreate, BatchExternalFeatureTableCreate],
        parent_batch_feature_table_name: Optional[str] = None,
    ) -> Task:
        """
        Create BatchFeatureTable by submitting an async prediction request task

        Parameters
        ----------
        data: Union[BatchFeatureTableCreate, BatchExternalFeatureTableCreate]
            Request parameters
        parent_batch_feature_table_name: Optional[str]
            Parent BatchFeatureTable name

        Returns
        -------
        Task
        """
        if data.batch_request_table_id:
            # Validate the batch_request_table_id
            batch_request_table = await self.batch_request_table_service.get_document(
                document_id=data.batch_request_table_id
            )
        else:
            batch_request_table = None

        # Validate the deployment_id
        deployment = await self.deployment_service.get_document(document_id=data.deployment_id)
        feature_list = await self.feature_list_service.get_document(
            document_id=deployment.feature_list_id
        )
        feature_store = await self.feature_store_service.get_document(
            document_id=data.feature_store_id
        )

        # get output columns and dtypes of the request input
        if batch_request_table is not None:
            input_columns_and_dtypes = {
                col.name: col.dtype for col in batch_request_table.columns_info
            }
        else:
            assert data.request_input is not None
            db_session = await self.feature_store_warehouse_service.session_manager_service.get_feature_store_session(
                feature_store=feature_store
            )
            (
                _,
                input_columns_and_dtypes,
            ) = await data.request_input.get_output_columns_and_dtypes(db_session)

        if deployment.serving_entity_ids:
            # Validate the entity of the batch request table matches the deployment's serving entity
            await self.entity_validation_service.validate_request_columns(
                columns_and_dtypes=input_columns_and_dtypes,
                serving_entity_ids=deployment.serving_entity_ids,
            )
        else:
            await self.entity_validation_service.validate_entities_or_prepare_for_parent_serving(
                feature_list_model=feature_list,
                request_column_names=set(input_columns_and_dtypes.keys()),
                feature_store=feature_store,
            )

        if isinstance(data, BatchExternalFeatureTableCreate):
            # include feature names and dtypes from the feature list
            async for doc in self.feature_service.list_documents_as_dict_iterator(
                projection={"name": 1, "dtype": 1},
                query_filter={"_id": {"$in": feature_list.feature_ids}},
            ):
                input_columns_and_dtypes[doc["name"]] = doc["dtype"]

            # prepare task payload for appending batch features to an unmanaged feature table
            payload = await self.batch_external_feature_table_service.get_batch_feature_table_task_payload(
                data=data,
                output_columns_and_dtypes=input_columns_and_dtypes,
            )
        else:
            # prepare task payload for creating a batch feature table
            payload = await self.service.get_batch_feature_table_task_payload(
                data=data, parent_batch_feature_table_name=parent_batch_feature_table_name
            )

        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))

    async def get_info(self, document_id: ObjectId, verbose: bool) -> BatchFeatureTableInfo:
        """
        Get BatchFeatureTable info

        Parameters
        ----------
        document_id: ObjectId
            BatchFeatureTable ID
        verbose: bool
            Whether to return verbose info

        Returns
        -------
        BatchFeatureTableInfo
        """
        _ = verbose
        batch_feature_table = await self.service.get_document(document_id=document_id)
        if batch_feature_table.batch_request_table_id:
            batch_request_table = await self.batch_request_table_service.get_document(
                document_id=batch_feature_table.batch_request_table_id
            )
            batch_request_table_name = batch_request_table.name
        else:
            assert batch_feature_table.request_input is not None
            batch_request_table_name = None

        deployment = await self.deployment_service.get_document(
            document_id=batch_feature_table.deployment_id
        )
        return BatchFeatureTableInfo(
            name=batch_feature_table.name,
            deployment_name=deployment.name,
            batch_request_table_name=batch_request_table_name,
            table_details=batch_feature_table.location.table_details,
            created_at=batch_feature_table.created_at,
            updated_at=batch_feature_table.updated_at,
            description=batch_feature_table.description,
        )

    async def recreate_batch_feature_table(
        self,
        batch_feature_table_id: ObjectId,
    ) -> Task:
        """
        Recreate BatchFeatureTable by submitting an async prediction request task

        Parameters
        ----------
        batch_feature_table_id : ObjectId
            The id of the BatchFeatureTable to recreate

        Raises
        ------
        FeatureTableRequestInputNotFoundError
            If request input is not found for the batch feature table

        Returns
        -------
        Task
        """
        batch_feature_table = await self.service.get_document(document_id=batch_feature_table_id)
        if batch_feature_table.request_input is None:
            # Request input not available in older batch feature tables
            raise FeatureTableRequestInputNotFoundError(
                "Request input not found for the batch feature table"
            )

        parent_batch_feature_table_name = (
            batch_feature_table.parent_batch_feature_table_name or batch_feature_table.name
        )

        assert self.service.catalog_id is not None
        catalog = await self.catalog_service.get_document(document_id=self.service.catalog_id)
        data: BatchFeatureTableCreate = BatchFeatureTableCreate(
            name=f"{parent_batch_feature_table_name} [{datetime.utcnow().isoformat()}]",
            feature_store_id=catalog.default_feature_store_ids[0],
            request_input=batch_feature_table.request_input,
            deployment_id=batch_feature_table.deployment_id,
        )
        return await self.create_batch_feature_table(
            data=data, parent_batch_feature_table_name=parent_batch_feature_table_name
        )
