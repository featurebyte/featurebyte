"""
BatchFeatureTableService class
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.enum import MaterializedTableNamePrefix
from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.persistent import Persistent
from featurebyte.schema.batch_feature_table import BatchFeatureTableCreate
from featurebyte.schema.info import BatchFeatureTableInfo
from featurebyte.schema.worker.task.batch_feature_table import BatchFeatureTableTaskPayload
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.materialized_table import BaseMaterializedTableService


class BatchFeatureTableService(
    BaseMaterializedTableService[BatchFeatureTableModel, BatchFeatureTableModel]
):
    """
    BatchFeatureTableService class
    """

    document_class = BatchFeatureTableModel
    materialized_table_name_prefix = MaterializedTableNamePrefix.BATCH_FEATURE_TABLE

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        feature_store_service: FeatureStoreService,
        deployment_service: DeploymentService,
        batch_request_table_service: BatchRequestTableService,
    ):
        super().__init__(user, persistent, catalog_id, feature_store_service)
        self.deployment_service = deployment_service
        self.batch_request_table_service = batch_request_table_service

    @property
    def class_name(self) -> str:
        return "BatchFeatureTable"

    async def get_batch_feature_table_task_payload(
        self, data: BatchFeatureTableCreate
    ) -> BatchFeatureTableTaskPayload:
        """
        Validate and convert a BatchFeatureTableCreate schema to a BatchFeatureTableTaskPayload schema
        which will be used to initiate the BatchFeatureTable creation task.

        Parameters
        ----------
        data: BatchFeatureTableCreate
            BatchFeatureTable creation payload

        Returns
        -------
        BatchFeatureTableTaskPayload
        """

        # Check any conflict with existing documents
        output_document_id = data.id or ObjectId()
        await self._check_document_unique_constraints(
            document=FeatureByteBaseDocumentModel(_id=output_document_id, name=data.name),
        )

        return BatchFeatureTableTaskPayload(
            **data.dict(),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
        )

    async def get_batch_feature_table_info(
        self, document_id: ObjectId, verbose: bool
    ) -> BatchFeatureTableInfo:
        """
        Get batch feature table info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        BatchFeatureTableInfo
        """
        _ = verbose
        batch_feature_table = await self.get_document(document_id=document_id)
        batch_request_table = await self.batch_request_table_service.get_document(
            document_id=batch_feature_table.batch_request_table_id
        )
        deployment = await self.deployment_service.get_document(
            document_id=batch_feature_table.deployment_id
        )
        return BatchFeatureTableInfo(
            name=batch_feature_table.name,
            deployment_name=deployment.name,
            batch_request_table_name=batch_request_table.name,
            table_details=batch_feature_table.location.table_details,
            created_at=batch_feature_table.created_at,
            updated_at=batch_feature_table.updated_at,
        )
