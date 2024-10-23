"""
TargetTableService class
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import pandas as pd
from bson import ObjectId
from redis import Redis

from featurebyte.enum import MaterializedTableNamePrefix
from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.persistent import QueryFilter
from featurebyte.models.target_table import TargetTableModel
from featurebyte.persistent import Persistent
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.target_table import TargetTableCreate
from featurebyte.schema.worker.task.target_table import TargetTableTaskPayload
from featurebyte.service.entity import EntityService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.materialized_table import BaseMaterializedTableService
from featurebyte.service.session_manager import SessionManagerService
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
        entity_service: EntityService,
        session_manager_service: SessionManagerService,
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

    @property
    def class_name(self) -> str:
        return "TargetTable"

    async def construct_get_query_filter(
        self, document_id: ObjectId, use_raw_query_filter: bool = False, **kwargs: Any
    ) -> QueryFilter:
        query_filter = await super().construct_get_query_filter(
            document_id=document_id, use_raw_query_filter=use_raw_query_filter, **kwargs
        )
        query_filter["request_input.type"] = {"$in": ["observation_table", "dataframe"]}
        return query_filter

    async def construct_list_query_filter(
        self,
        query_filter: Optional[QueryFilter] = None,
        use_raw_query_filter: bool = False,
        **kwargs: Any,
    ) -> QueryFilter:
        query_filter = await super().construct_list_query_filter(
            query_filter=query_filter, use_raw_query_filter=use_raw_query_filter, **kwargs
        )
        query_filter["request_input.type"] = {"$in": ["observation_table", "dataframe"]}
        return query_filter

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
            **data.model_dump(),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
            observation_set_storage_path=observation_set_storage_path,
        )
