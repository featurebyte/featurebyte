"""
HistoricalFeatureTableService class
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import pandas as pd
from bson import ObjectId
from redis import Redis

from featurebyte.enum import MaterializedTableNamePrefix
from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.historical_feature_table import HistoricalFeatureTableModel
from featurebyte.persistent import Persistent
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.constant import MAX_BATCH_FEATURE_ITEM_COUNT
from featurebyte.schema.historical_feature_table import HistoricalFeatureTableCreate
from featurebyte.schema.worker.task.historical_feature_table import (
    HistoricalFeatureTableTaskPayload,
)
from featurebyte.service.entity import EntityService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.materialized_table import BaseMaterializedTableService
from featurebyte.service.session_manager import SessionManagerService
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
        session_manager_service: SessionManagerService,
        entity_service: EntityService,
        temp_storage: Storage,
        block_modification_handler: BlockModificationHandler,
        storage: Storage,
        redis: Redis[Any],
    ):
        super().__init__(
            user,
            persistent,
            catalog_id,
            session_manager_service,
            feature_store_service,
            entity_service,
            block_modification_handler,
            storage,
            redis,
        )
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

        Raises
        ------
        ValueError
            If the number of features exceeds the limit
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

        # check number of features whether exceeds the limit
        # if the task is triggered by a saved feature list, this limit is not applied.
        # if the feature list saved, the feature_clusters will be empty
        # (based on the payload construction logic in SDK compute_historical_feature_table method).
        feature_clusters = data.featurelist_get_historical_features.feature_clusters
        if feature_clusters:
            num_features = 0
            for feature_cluster in feature_clusters:
                num_features += len(feature_cluster.node_names)

            if num_features > MAX_BATCH_FEATURE_ITEM_COUNT:
                raise ValueError(
                    f"Number of features exceeds the limit of {MAX_BATCH_FEATURE_ITEM_COUNT}, "
                    "please reduce the number of features or save the features in a feature list and try again."
                )

        return HistoricalFeatureTableTaskPayload(
            **data.model_dump(),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
            observation_set_storage_path=observation_set_storage_path,
        )
