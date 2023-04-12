"""
ModelingTableService class
"""
from __future__ import annotations

from bson import ObjectId

from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.modeling_table import ModelingTableModel
from featurebyte.schema.modeling_table import ModelingTableCreate
from featurebyte.schema.worker.task.modeling_table import ModelingTableTaskPayload
from featurebyte.service.materialized_table import BaseMaterializedTableService


class ModelingTableService(BaseMaterializedTableService[ModelingTableModel, ModelingTableModel]):
    """
    ModelingTableService class
    """

    document_class = ModelingTableModel
    materialized_table_name_prefix = "MODELING_TABLE"

    @property
    def class_name(self) -> str:
        return "ModelingTable"

    async def get_modeling_table_task_payload(
        self, data: ModelingTableCreate
    ) -> ModelingTableTaskPayload:
        """
        Validate and convert a ModelingTableCreate schema to a ModelingTableTaskPayload schema
        which will be used to initiate the ModelingTable creation task.

        Parameters
        ----------
        data: ModelingTableCreate
            ModelingTable creation payload

        Returns
        -------
        ModelingTableTaskPayload
        """

        # Check any conflict with existing documents
        output_document_id = data.id or ObjectId()
        await self._check_document_unique_constraints(
            document=FeatureByteBaseDocumentModel(_id=output_document_id, name=data.name),
        )

        return ModelingTableTaskPayload(
            **data.dict(),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
        )
