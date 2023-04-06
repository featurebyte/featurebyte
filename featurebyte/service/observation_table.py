"""
ObservationTableService class
"""
from __future__ import annotations

from bson import ObjectId

from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.schema.observation_table import ObservationTableCreate
from featurebyte.schema.worker.task.observation_table import ObservationTableTaskPayload
from featurebyte.service.base_table_document import BaseDocumentService
from featurebyte.service.context import ContextService


class ObservationTableService(
    BaseDocumentService[
        ObservationTableModel, ObservationTableModel, BaseDocumentServiceUpdateSchema
    ]
):
    """
    ObservationTableService class
    """

    document_class = ObservationTableModel

    @property
    def class_name(self) -> str:
        return "ObservationTable"

    async def get_observation_table_task_payload(
        self, data: ObservationTableCreate
    ) -> ObservationTableTaskPayload:

        # Check any conflict with existing documents
        output_document_id = data.id or ObjectId()
        await self._check_document_unique_constraints(
            document=FeatureByteBaseDocumentModel(_id=output_document_id, name=data.name),
        )

        if data.context_id is not None:
            # Check if the context document exists when provided. This should perform additional
            # validation once additional information such as request schema are available in the
            # context.
            context_service = ContextService(
                user=self.user, persistent=self.persistent, catalog_id=self.catalog_id
            )
            await context_service.get_document(document_id=data.context_id)

        return ObservationTableTaskPayload(
            **data.dict(),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
        )
