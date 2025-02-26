"""
BatchFeatureTableService class
"""

from __future__ import annotations

from typing import Optional

from bson import ObjectId

from featurebyte.enum import MaterializedTableNamePrefix
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
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
        self,
        data: BatchFeatureTableCreate,
        parent_batch_feature_table_name: Optional[str] = None,
    ) -> BatchFeatureTableTaskPayload:
        """
        Validate and convert a BatchFeatureTableCreate schema to a BatchFeatureTableTaskPayload schema
        which will be used to initiate the BatchFeatureTable creation task.

        Parameters
        ----------
        data: BatchFeatureTableCreate
            BatchFeatureTable creation payload
        parent_batch_feature_table_name: Optional[str]
            Parent BatchFeatureTable name

        Returns
        -------
        BatchFeatureTableTaskPayload
        """

        # Check any conflict with existing documents
        output_document_id = data.id or ObjectId()
        await self._check_document_unique_constraints(
            document=BatchFeatureTableModel(
                _id=output_document_id,
                name=data.name,
                deployment_id=data.deployment_id,
                batch_request_table_id=None,
                request_input=None,
                location=TabularSource(
                    feature_store_id=ObjectId(),
                    table_details=TableDetails(table_name="table_name"),
                ),
                columns_info=[],
                num_rows=0,
            ),
        )

        return BatchFeatureTableTaskPayload(
            **data.model_dump(by_alias=True),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
            parent_batch_feature_table_name=parent_batch_feature_table_name,
        )
