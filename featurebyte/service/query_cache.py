"""
QueryCacheService class
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any, AsyncIterator, Dict

from bson import ObjectId

from featurebyte.models.query_cache import QueryCacheModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.service.base_document import BaseDocumentService

STALE_THRESHOLD_DAYS = int(os.getenv("FEATUREBYTE_QUERY_CACHE_THRESHOLD_DAYS", "21"))
STALE_THRESHOLD_SECONDS = 86400 * STALE_THRESHOLD_DAYS
STALE_CLEANUP_BUFFER_SECONDS = 3600 * 3


class QueryCacheDocumentService(
    BaseDocumentService[QueryCacheModel, QueryCacheModel, BaseDocumentServiceUpdateSchema]
):
    """
    QueryCacheService class
    """

    document_class = QueryCacheModel

    async def list_stale_documents_as_dict_iterator(
        self, feature_store_id: ObjectId
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        List stale documents iterator

        Parameters
        ----------
        feature_store_id: ObjectId
            feature store id

        Yields
        ------
        AsyncIterator[Dict[str, Any]]
            iterator of stale documents
        """
        # Clean up stale documents with a buffer to ensure that cached table is not being used
        # during cleanup
        stale_timestamp = datetime.utcnow() - timedelta(
            seconds=(STALE_THRESHOLD_SECONDS + STALE_CLEANUP_BUFFER_SECONDS)
        )
        query_filter = {
            "feature_store_id": ObjectId(feature_store_id),
            "created_at": {"$lt": stale_timestamp},
        }
        async for doc in self.list_documents_as_dict_iterator(query_filter=query_filter):
            yield doc

    @classmethod
    def get_cache_validity_filter(cls) -> Dict[str, Any]:
        """
        Get cache validity filter

        Returns
        -------
        Dict[str, Any]
            cache validity filter
        """
        earliest_valid_timestamp = datetime.utcnow() - timedelta(seconds=STALE_THRESHOLD_SECONDS)
        return {"created_at": {"$gte": earliest_valid_timestamp}}
