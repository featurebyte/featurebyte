"""
ObservationTableService class
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.persistent import Persistent
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.schema.observation_table import ObservationTableCreate
from featurebyte.schema.worker.task.observation_table import ObservationTableTaskPayload
from featurebyte.service.base_document import BaseDocumentService
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

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        context_service: ContextService,
    ):
        super().__init__(user, persistent, catalog_id)
        self.context_service = context_service

    @property
    def class_name(self) -> str:
        return "ObservationTable"

    async def get_observation_table_task_payload(
        self, data: ObservationTableCreate
    ) -> ObservationTableTaskPayload:
        """
        Validate and convert a ObservationTableCreate schema to a ObservationTableTaskPayload schema
        which will be used to initiate the ObservationTable creation task.

        Parameters
        ----------
        data: ObservationTableCreate
            ObservationTable creation payload

        Returns
        -------
        ObservationTableTaskPayload
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

        return ObservationTableTaskPayload(
            **data.dict(),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
        )
