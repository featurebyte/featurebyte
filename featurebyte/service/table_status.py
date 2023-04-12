"""
TableStatusService class
"""
from bson import ObjectId

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.feature_store import TableStatus
from featurebyte.service.base_service import BaseService
from featurebyte.service.table_columns_info import TableDocumentService


class TableStatusService(BaseService):
    """TableStatusService class"""

    @staticmethod
    async def update_status(
        service: TableDocumentService, document_id: ObjectId, status: TableStatus
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
            eligible_transitions = {
                TableStatus.PUBLIC_DRAFT: {TableStatus.PUBLISHED, TableStatus.DEPRECATED},
                TableStatus.PUBLISHED: {TableStatus.DEPRECATED},
                TableStatus.DEPRECATED: {},
            }
            if status not in eligible_transitions[current_status]:
                raise DocumentUpdateError(
                    f"Invalid status transition from {current_status} to {status}."
                )
            await service.update_document(
                document_id=document_id,
                data=service.document_update_class(status=status),  # type: ignore
                return_document=False,
            )
