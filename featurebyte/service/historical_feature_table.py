"""
HistoricalFeatureTableService class
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.historical_feature_table import HistoricalFeatureTableModel
from featurebyte.persistent import Persistent
from featurebyte.schema.historical_feature_table import HistoricalFeatureTableCreate
from featurebyte.schema.worker.task.historical_feature_table import (
    HistoricalFeatureTableTaskPayload,
)
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.materialized_table import BaseMaterializedTableService
from featurebyte.service.observation_table import ObservationTableService


class HistoricalFeatureTableService(
    BaseMaterializedTableService[HistoricalFeatureTableModel, HistoricalFeatureTableModel]
):
    """
    HistoricalFeatureTableService class
    """

    document_class = HistoricalFeatureTableModel
    materialized_table_name_prefix = "HISTORICAL_FEATURE_TABLE"

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
        self, data: HistoricalFeatureTableCreate
    ) -> HistoricalFeatureTableTaskPayload:
        """
        Validate and convert a HistoricalFeatureTableCreate schema to a HistoricalFeatureTableTaskPayload schema
        which will be used to initiate the HistoricalFeatureTable creation task.

        Parameters
        ----------
        data: HistoricalFeatureTableCreate
            HistoricalFeatureTable creation payload

        Returns
        -------
        HistoricalFeatureTableTaskPayload
        """

        # Check any conflict with existing documents
        output_document_id = data.id or ObjectId()
        await self._check_document_unique_constraints(
            document=FeatureByteBaseDocumentModel(_id=output_document_id, name=data.name),
        )

        # Validate the observation_table_id
        await self.observation_table_service.get_document(document_id=data.observation_table_id)

        return HistoricalFeatureTableTaskPayload(
            **data.dict(),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
        )
