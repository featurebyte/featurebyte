"""
PeriodicTaskService class
"""
from __future__ import annotations

from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.periodic_task import PeriodicTask
from featurebyte.schema.periodic_task import PeriodicTaskUpdate
from featurebyte.service.base_document import BaseDocumentService


class PeriodicTaskService(BaseDocumentService[PeriodicTask, PeriodicTask, PeriodicTaskUpdate]):
    """
    PeriodicTaskService class
    """

    document_class = PeriodicTask

    async def delete_document_by_name(self, name: str) -> None:
        """
        Delete document

        Parameters
        ----------
        name: str
            Document Name
        """
        await self.persistent.delete_one(
            collection_name=self.collection_name,
            query_filter={"name": name, "workspace_id": self.workspace_id},
            user_id=self.user.id,
        )

    async def get_document_by_name(
        self,
        name: str,
    ) -> PeriodicTask:
        """
        Retrieve document

        Parameters
        ----------
        name: str
            Document Name

        Raises
        -------
        DocumentNotFoundError
            If document is not found

        Returns
        -------
        Document
        """
        document = await self.persistent.find_one(
            collection_name=self.collection_name,
            query_filter={"name": name},
            user_id=self.user.id,
        )

        if not document:
            raise DocumentNotFoundError(f"Document with name {name} not found")

        return self.document_class(**document)
