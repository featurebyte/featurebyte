"""
BatchRequestTableService class
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.enum import MaterializedTableNamePrefix
from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.persistent import Persistent
from featurebyte.schema.batch_request_table import BatchRequestTableCreate
from featurebyte.schema.info import BatchRequestTableInfo
from featurebyte.schema.worker.task.batch_request_table import BatchRequestTableTaskPayload
from featurebyte.service.context import ContextService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.materialized_table import BaseMaterializedTableService


class BatchRequestTableService(
    BaseMaterializedTableService[BatchRequestTableModel, BatchRequestTableModel]
):
    """
    BatchRequestTableService class
    """

    document_class = BatchRequestTableModel
    materialized_table_name_prefix = MaterializedTableNamePrefix.BATCH_REQUEST_TABLE

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        feature_store_service: FeatureStoreService,
        context_service: ContextService,
    ):
        super().__init__(user, persistent, catalog_id, feature_store_service)
        self.context_service = context_service

    @property
    def class_name(self) -> str:
        return "BatchRequestTable"

    async def get_batch_request_table_task_payload(
        self, data: BatchRequestTableCreate
    ) -> BatchRequestTableTaskPayload:
        """
        Validate and convert a BatchRequestTableCreate schema to a BatchRequestTableTaskPayload schema
        which will be used to initiate the BatchRequestTable creation task.

        Parameters
        ----------
        data: BatchRequestTableCreate
            BatchRequestTable creation payload

        Returns
        -------
        BatchRequestTableTaskPayload
        """

        # Check any conflict with existing documents
        output_document_id = data.id or ObjectId()
        await self._check_document_unique_constraints(
            document=FeatureByteBaseDocumentModel(_id=output_document_id, name=data.name),
        )

        if data.context_id is not None:
            # Check if the context document exists when provided. This should perform additional
            # validation once additional information such as request schema are available in the
            # context.
            await self.context_service.get_document(document_id=data.context_id)

        return BatchRequestTableTaskPayload(
            **data.dict(by_alias=True),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
        )

    async def get_batch_request_table_info(
        self, document_id: ObjectId, verbose: bool
    ) -> BatchRequestTableInfo:
        """
        Get batch request table info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        BatchRequestTableInfo
        """
        _ = verbose
        batch_request_table = await self.get_document(document_id=document_id)
        feature_store = await self.feature_store_service.get_document(
            document_id=batch_request_table.location.feature_store_id
        )
        return BatchRequestTableInfo(
            name=batch_request_table.name,
            type=batch_request_table.request_input.type,
            feature_store_name=feature_store.name,
            table_details=batch_request_table.location.table_details,
            created_at=batch_request_table.created_at,
            updated_at=batch_request_table.updated_at,
        )
