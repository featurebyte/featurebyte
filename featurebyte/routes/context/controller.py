"""
Context API route controller
"""
from __future__ import annotations

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
