"""
PredictionTableService class
"""
from __future__ import annotations

from bson import ObjectId

from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.prediction_table import PredictionTableModel
from featurebyte.schema.prediction_table import PredictionTableCreate
from featurebyte.schema.worker.task.prediction_table import PredictionTableTaskPayload
from featurebyte.service.materialized_table import BaseMaterializedTableService


class PredictionTableService(
    BaseMaterializedTableService[PredictionTableModel, PredictionTableModel]
):
    """
    PredictionTableService class
    """

    document_class = PredictionTableModel
    materialized_table_name_prefix = "PREDICTION_TABLE"

    @property
    def class_name(self) -> str:
        return "PredictionTable"

    async def get_prediction_table_task_payload(
        self, data: PredictionTableCreate
    ) -> PredictionTableTaskPayload:
        """
        Validate and convert a PredictionTableCreate schema to a PredictionTableTaskPayload schema
        which will be used to initiate the PredictionTable creation task.

        Parameters
        ----------
        data: PredictionTableCreate
            PredictionTable creation payload

        Returns
        -------
        PredictionTableTaskPayload
        """

        # Check any conflict with existing documents
        output_document_id = data.id or ObjectId()
        await self._check_document_unique_constraints(
            document=FeatureByteBaseDocumentModel(_id=output_document_id, name=data.name),
        )

        return PredictionTableTaskPayload(
            **data.dict(),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
        )
