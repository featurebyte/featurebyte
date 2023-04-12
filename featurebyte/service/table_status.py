"""
TableStatusService class
"""
from typing import Any

from bson import ObjectId

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.feature import FeatureReadiness
from featurebyte.models.feature_store import TableStatus
from featurebyte.persistent import Persistent
from featurebyte.service.base_service import BaseService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_readiness import FeatureReadinessService
from featurebyte.service.table_columns_info import TableDocumentService


class TableStatusService(BaseService):
    """TableStatusService class"""

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        feature_service: FeatureService,
        feature_readiness_service: FeatureReadinessService,
    ):
        super().__init__(user, persistent, catalog_id)
        self.feature_service = feature_service
        self.feature_readiness_service = feature_readiness_service

    async def quarantine_affected_features(self, table_id: ObjectId) -> None:
        """
        Quarantine features affected by deprecation of a table

        Parameters
        ----------
        table_id: ObjectId
            Deprecated table ID
        """
        async for feature_doc in self.feature_service.list_documents_iterator(
            query_filter={"table_ids": {"$in": [table_id]}}
        ):
            await self.feature_readiness_service.update_feature(
                feature_id=feature_doc["_id"],
                readiness=FeatureReadiness.QUARANTINE,
                return_document=False,
            )

    async def update_status(
        self, service: TableDocumentService, document_id: ObjectId, status: TableStatus
    ) -> None:
        """
        Update table status

        Parameters
        ----------
        service: TableDocumentService
            Table service object
        document_id: ObjectId
            Document ID
        status: TableStatus
            Table status to be updated

        Raises
        ------
        DocumentUpdateError
            When the table status transition is invalid
        """
        document = await service.get_document(document_id=document_id)

        current_status = document.status
        if current_status != status:
            # check eligibility of status transition
            if current_status == TableStatus.DEPRECATED:
                raise DocumentUpdateError(
                    f"Invalid status transition from {current_status} to {status}."
                )

            async with self.persistent.start_transaction():
                # update table status
                await service.update_document(
                    document_id=document_id,
                    data=service.document_update_class(status=status),  # type: ignore
                    return_document=False,
                )

                if status == TableStatus.DEPRECATED:
                    # quarantine features affected by deprecation of a table
                    await self.quarantine_affected_features(table_id=document_id)
