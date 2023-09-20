"""
Context API route controller
"""
from __future__ import annotations

from bson import ObjectId

from featurebyte.models.context import ContextModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.context import ContextList, ContextUpdate
from featurebyte.schema.observation_table import ObservationTableUpdate
from featurebyte.service.context import ContextService
from featurebyte.service.observation_table import ObservationTableService


class ContextController(BaseDocumentController[ContextModel, ContextService, ContextList]):
    """
    Context controller
    """

    paginated_document_class = ContextList
    document_update_schema_class = ContextUpdate

    def __init__(
        self,
        observation_table_service: ObservationTableService,
        context_service: ContextService,
    ):
        super().__init__(service=context_service)
        self.observation_table_service = observation_table_service
        self.context_service = context_service

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
        if data.default_preview_table_id:
            await self.observation_table_service.update_observation_table(
                observation_table_id=data.default_preview_table_id,
                data=ObservationTableUpdate(context_id=context_id),
            )

        if data.default_eda_table_id:
            await self.observation_table_service.update_observation_table(
                observation_table_id=data.default_eda_table_id,
                data=ObservationTableUpdate(context_id=context_id),
            )

        await self.service.update_document(
            document_id=context_id, data=ContextUpdate(**data.dict()), return_document=False
        )
        return await self.get(document_id=context_id)
