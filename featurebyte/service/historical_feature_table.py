"""
HistoricalFeatureTableService class
"""
from __future__ import annotations

from typing import Any, Optional

from pathlib import Path

import pandas as pd
from bson import ObjectId

from featurebyte.enum import MaterializedTableNamePrefix
from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.historical_feature_table import HistoricalFeatureTableModel
from featurebyte.persistent import Persistent
from featurebyte.schema.historical_feature_table import HistoricalFeatureTableCreate
from featurebyte.schema.worker.task.historical_feature_table import (
    HistoricalFeatureTableTaskPayload,
)
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.materialized_table import BaseMaterializedTableService
from featurebyte.storage import Storage


class HistoricalFeatureTableService(
    BaseMaterializedTableService[HistoricalFeatureTableModel, HistoricalFeatureTableModel]
):
    """
    HistoricalFeatureTableService class
    """

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

    document_class = HistoricalFeatureTableModel
    materialized_table_name_prefix = MaterializedTableNamePrefix.HISTORICAL_FEATURE_TABLE

    @property
    def class_name(self) -> str:
        return "HistoricalFeatureTable"

    async def get_historical_feature_table_task_payload(
        self,
        data: HistoricalFeatureTableCreate,
        observation_set_dataframe: Optional[pd.DataFrame],
    ) -> HistoricalFeatureTableTaskPayload:
        """
        Validate and convert a HistoricalFeatureTableCreate schema to a HistoricalFeatureTableTaskPayload schema
        which will be used to initiate the HistoricalFeatureTable creation task.

        Parameters
        ----------
        data: HistoricalFeatureTableCreate
            HistoricalFeatureTable creation payload
        observation_set_dataframe: Optional[pd.DataFrame]
            Optional observation set DataFrame. If provided, the DataFrame will be stored in the
            temp storage to be used by the HistoricalFeatureTable creation task.

        Returns
        -------
        HistoricalFeatureTableTaskPayload
        """

        # Check any conflict with existing documents
        output_document_id = data.id or ObjectId()
        await self._check_document_unique_constraints(
            document=FeatureByteBaseDocumentModel(_id=output_document_id, name=data.name),
        )

        if observation_set_dataframe is not None:
            observation_set_storage_path = (
                f"historical_feature_table/observation_set/{output_document_id}.parquet"
            )
            await self.temp_storage.put_dataframe(
                observation_set_dataframe, Path(observation_set_storage_path)
            )
        else:
            observation_set_storage_path = None

        return HistoricalFeatureTableTaskPayload(
            **data.dict(),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
            observation_set_storage_path=observation_set_storage_path,
        )
