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
from featurebyte.schema.info import HistoricalFeatureTableInfo
from featurebyte.schema.worker.task.historical_feature_table import (
    HistoricalFeatureTableTaskPayload,
)
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.materialized_table import BaseMaterializedTableService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.storage import Storage


class HistoricalFeatureTableService(
    BaseMaterializedTableService[HistoricalFeatureTableModel, HistoricalFeatureTableModel]
):
    """
    HistoricalFeatureTableService class
    """

    document_class = HistoricalFeatureTableModel
    materialized_table_name_prefix = MaterializedTableNamePrefix.HISTORICAL_FEATURE_TABLE

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        feature_store_service: FeatureStoreService,
        observation_table_service: ObservationTableService,
        feature_list_service: FeatureListService,
    ):
        super().__init__(user, persistent, catalog_id, feature_store_service)
        self.observation_table_service = observation_table_service
        self.feature_list_service = feature_list_service

    @property
    def class_name(self) -> str:
        return "HistoricalFeatureTable"

    async def get_historical_feature_table_task_payload(
        self,
        data: HistoricalFeatureTableCreate,
        storage: Storage,
        observation_set_dataframe: Optional[pd.DataFrame],
    ) -> HistoricalFeatureTableTaskPayload:
        """
        Validate and convert a HistoricalFeatureTableCreate schema to a HistoricalFeatureTableTaskPayload schema
        which will be used to initiate the HistoricalFeatureTable creation task.

        Parameters
        ----------
        data: HistoricalFeatureTableCreate
            HistoricalFeatureTable creation payload
        storage: Storage
            Storage instance
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
            await storage.put_dataframe(
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

    async def get_historical_feature_table_info(
        self, document_id: ObjectId, verbose: bool
    ) -> HistoricalFeatureTableInfo:
        """
        Get historical feature table info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        HistoricalFeatureTableInfo
        """
        _ = verbose
        historical_feature_table = await self.get_document(document_id=document_id)
        if historical_feature_table.observation_table_id is not None:
            observation_table = await self.observation_table_service.get_document(
                document_id=historical_feature_table.observation_table_id
            )
        else:
            observation_table = None
        feature_list = await self.feature_list_service.get_document(
            document_id=historical_feature_table.feature_list_id
        )
        return HistoricalFeatureTableInfo(
            name=historical_feature_table.name,
            feature_list_name=feature_list.name,
            feature_list_version=feature_list.version.to_str(),
            observation_table_name=observation_table.name if observation_table else None,
            table_details=historical_feature_table.location.table_details,
            created_at=historical_feature_table.created_at,
            updated_at=historical_feature_table.updated_at,
        )
