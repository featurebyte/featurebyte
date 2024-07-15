"""
BatchFeatureTableService class
"""

from __future__ import annotations

from bson import ObjectId

from featurebyte.enum import MaterializedTableNamePrefix
from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.schema.batch_feature_table import BatchFeatureTableCreate
from featurebyte.schema.worker.task.batch_feature_table import BatchFeatureTableTaskPayload
from featurebyte.service.materialized_table import BaseMaterializedTableService


class BatchFeatureTableService(
    BaseMaterializedTableService[BatchFeatureTableModel, BatchFeatureTableModel]
):
    """
    BatchFeatureTableService class
    """

    document_class = BatchFeatureTableModel
    materialized_table_name_prefix = MaterializedTableNamePrefix.BATCH_FEATURE_TABLE

    @property
    def class_name(self) -> str:
        return "BatchFeatureTable"

    async def get_batch_feature_table_task_payload(
        self, data: BatchFeatureTableCreate
    ) -> BatchFeatureTableTaskPayload:
        """
        Validate and convert a BatchFeatureTableCreate schema to a BatchFeatureTableTaskPayload schema
        which will be used to initiate the BatchFeatureTable creation task.

        Parameters
        ----------
        data: BatchFeatureTableCreate
            BatchFeatureTable creation payload

        Returns
        -------
        BatchFeatureTableTaskPayload
        """

        # Check any conflict with existing documents
        output_document_id = data.id or ObjectId()
        await self._check_document_unique_constraints(
            document=FeatureByteBaseDocumentModel(_id=output_document_id, name=data.name),
        )

        return BatchFeatureTableTaskPayload(
            **data.dict(),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
        )
