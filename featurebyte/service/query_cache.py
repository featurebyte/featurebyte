"""
QueryCacheService class
"""

from __future__ import annotations

from typing import Any, AsyncIterator, Dict

from datetime import datetime, timedelta

from bson import ObjectId

from featurebyte.models.query_cache import QueryCacheModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.service.base_document import BaseDocumentService

STALE_THRESHOLD_SECONDS = 86400 * 7


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
        stale_timestamp = datetime.utcnow() - timedelta(seconds=STALE_THRESHOLD_SECONDS)
        query_filter = {
            "feature_store_id": ObjectId(feature_store_id),
            "created_at": {"$lt": stale_timestamp},
        }
        async for doc in self.list_documents_as_dict_iterator(query_filter=query_filter):
            yield doc
