"""
BatchRequestTableService class
"""

from __future__ import annotations

from typing import Any, Optional

from bson import ObjectId
from redis import Redis

from featurebyte.enum import MaterializedTableNamePrefix
from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.persistent import Persistent
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.batch_request_table import BatchRequestTableCreate
from featurebyte.schema.worker.task.batch_request_table import BatchRequestTableTaskPayload
from featurebyte.service.context import ContextService
from featurebyte.service.entity import EntityService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.materialized_table import BaseMaterializedTableService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.storage import Storage


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
        catalog_id: Optional[ObjectId],
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        entity_service: EntityService,
        context_service: ContextService,
        block_modification_handler: BlockModificationHandler,
        storage: Storage,
        redis: Redis[Any],
    ):
        super().__init__(
            user,
            persistent,
            catalog_id,
            session_manager_service,
            feature_store_service,
            entity_service,
            block_modification_handler,
            storage,
            redis,
        )
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
            **data.model_dump(by_alias=True),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
        )
