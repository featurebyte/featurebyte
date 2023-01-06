"""
Context API route controller
"""
from __future__ import annotations

from bson import ObjectId

from featurebyte.models.context import ContextModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.context import ContextCreate, ContextList, ContextUpdate
from featurebyte.service.context import ContextService


class ContextController(BaseDocumentController[ContextModel, ContextService, ContextList]):
    """
    Context controller
    """

    paginated_document_class = ContextList
    document_update_schema_class = ContextUpdate

    async def create_context(self, data: ContextCreate) -> ContextModel:
        """
        Create context at persistent

        Parameters
        ----------
        data: ContextCreate
            Context creation payload

        Returns
        -------
        ContextModel
            Newly created context object
        """
        return await self.service.create_document(data)

    async def update_context(self, context_id: ObjectId, data: ContextUpdate) -> ContextModel:
        """
        Update Context stored at persistent

        Parameters
        ----------
        context_id: ObjectId
            Context ID
        data: ContextUpdate
            Context update payload

        Returns
        -------
        ContextModel
            Context object with updated attribute(s)
        """
        await self.service.update_document(
            document_id=context_id, data=ContextUpdate(**data.dict()), return_document=False
        )
        return await self.get(document_id=context_id)
