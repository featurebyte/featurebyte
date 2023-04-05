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

        # check any conflict with existing documents
        output_document_id = data.id or ObjectId()
        await self._check_document_unique_constraints(
            document=FeatureByteBaseDocumentModel(_id=output_document_id, name=data.name),
        )

        return ObservationTableTaskPayload(
            **data.dict(),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
        )
