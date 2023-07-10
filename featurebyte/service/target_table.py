"""
TargetTableService class
"""
from __future__ import annotations

from typing import Optional

from pathlib import Path

import pandas as pd
from bson import ObjectId

from featurebyte.enum import MaterializedTableNamePrefix
from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.target_table import TargetTableModel
from featurebyte.schema.target_table import TargetTableCreate
from featurebyte.schema.worker.task.target_table import TargetTableTaskPayload
from featurebyte.service.materialized_table import BaseMaterializedTableService
from featurebyte.storage import Storage


class TargetTableService(BaseMaterializedTableService[TargetTableModel, TargetTableModel]):
    """
    TargetTableService class
    """

    document_class = TargetTableModel
    materialized_table_name_prefix = MaterializedTableNamePrefix.HISTORICAL_FEATURE_TABLE

    @property
    def class_name(self) -> str:
        return "TargetTable"

    async def get_target_table_task_payload(
        self,
        data: TargetTableCreate,
        storage: Storage,
        observation_set_dataframe: Optional[pd.DataFrame],
    ) -> TargetTableTaskPayload:
        """
        Validate and convert a TargetTableCreate schema to a TargetTableTaskPayload schema
        which will be used to initiate the TargetTable creation task.

        Parameters
        ----------
        data: TargetTableCreate
            TargetTable creation payload
        storage: Storage
            Storage instance
        observation_set_dataframe: Optional[pd.DataFrame]
            Optional observation set DataFrame. If provided, the DataFrame will be stored in the
            temp storage to be used by the TargetTable creation task.

        Returns
        -------
        TargetTableTaskPayload
        """

        # Check any conflict with existing documents
        output_document_id = data.id or ObjectId()
        await self._check_document_unique_constraints(
            document=FeatureByteBaseDocumentModel(_id=output_document_id, name=data.name),
        )

        if observation_set_dataframe is not None:
            observation_set_storage_path = (
                f"target_table/observation_set/{output_document_id}.parquet"
            )
            await storage.put_dataframe(
                observation_set_dataframe, Path(observation_set_storage_path)
            )
        else:
            observation_set_storage_path = None

        return TargetTableTaskPayload(
            **data.dict(),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
            observation_set_storage_path=observation_set_storage_path,
        )
