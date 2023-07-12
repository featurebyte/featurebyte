"""
TargetTableService class
"""
from __future__ import annotations

from typing import Any, Optional

from pathlib import Path

import pandas as pd
from bson import ObjectId

from featurebyte.enum import MaterializedTableNamePrefix
from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.target_table import TargetTableModel
from featurebyte.persistent import Persistent
from featurebyte.schema.target_table import TargetTableCreate
from featurebyte.schema.worker.task.target_table import TargetTableTaskPayload
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.materialized_table import BaseMaterializedTableService
from featurebyte.storage import Storage


class TargetTableService(BaseMaterializedTableService[TargetTableModel, TargetTableModel]):
    """
    TargetTableService class
    """

    document_class = TargetTableModel
    materialized_table_name_prefix = MaterializedTableNamePrefix.TARGET_TABLE

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        feature_store_service: FeatureStoreService,
        temp_storage: Storage,
    ):
        super().__init__(user, persistent, catalog_id, feature_store_service)
        self.temp_storage = temp_storage

    @property
    def class_name(self) -> str:
        return "TargetTable"

    async def get_target_table_task_payload(
        self,
        data: TargetTableCreate,
        observation_set_dataframe: Optional[pd.DataFrame],
    ) -> TargetTableTaskPayload:
        """
        Validate and convert a TargetTableCreate schema to a TargetTableTaskPayload schema
        which will be used to initiate the TargetTable creation task.

        Parameters
        ----------
        data: TargetTableCreate
            TargetTable creation payload
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
            await self.temp_storage.put_dataframe(
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
