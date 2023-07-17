"""
TileJobStatusService class
"""
from __future__ import annotations

import datetime
from collections import defaultdict

import pandas as pd

from featurebyte.models.tile import TileType
from featurebyte.models.tile_job_log import TileJobLogModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.service.base_document import BaseDocumentService


class TileJobLogService(
    BaseDocumentService[TileJobLogModel, TileJobLogModel, BaseDocumentServiceUpdateSchema]
):
    """
    TileJobLogService class

    This service is used by TileTaskExecutor and downstream steps to update the status of the tile
    jobs. It also supports reporting of tile jobs summary via FeatureStoreWarehouseService.
    """

    document_class = TileJobLogModel
    document_update_class = BaseDocumentServiceUpdateSchema

    async def get_logs_dataframe(self, aggregation_ids: list[str], hour_limit: int) -> pd.DataFrame:
        datetime_now = datetime.datetime.utcnow()
        min_created_at = datetime_now - datetime.timedelta(hours=hour_limit)
        query_filter = {
            "created_at": {"$gte": min_created_at, "$lt": datetime_now},
            "aggregation_id": {"$in": aggregation_ids},
            "tile_type": TileType.ONLINE.value,
        }
        columns = [
            "session_id",
            "created_at",
            "aggregation_id",
            "status",
            "message",
        ]
        data = defaultdict(list)
        async for doc in self.list_documents_as_dict_iterator(query_filter=query_filter):
            for col in columns:
                data[col.upper()].append(doc[col])
        return pd.DataFrame(data=data)
