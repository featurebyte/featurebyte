"""
PeriodicTaskService class
"""
from __future__ import annotations

from bson import ObjectId

from featurebyte.models.periodic_task import PeriodicTask
from featurebyte.schema.periodic_task import PeriodicTaskUpdate
from featurebyte.service.base_document import BaseDocumentService


class PeriodicTaskService(BaseDocumentService[PeriodicTask, PeriodicTask, PeriodicTaskUpdate]):
    """
    PeriodicTaskService class
    """

    document_class = PeriodicTask

    async def delete_document(self, document_id: ObjectId) -> None:
        """
        Delete document

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        """
        await self.persistent.delete_one(
            collection_name=self.collection_name,
            query_filter={"_id": document_id, "workspace_id": self.workspace_id},
            user_id=self.user.id,
        )
