"""
OnlineStoreTableVersionService class
"""

from __future__ import annotations

from typing import Dict, Optional

from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.online_store_table_version import (
    OnlineStoreTableVersion,
    OnlineStoreTableVersionUpdate,
)
from featurebyte.service.base_document import BaseDocumentService


class OnlineStoreTableVersionService(
    BaseDocumentService[
        OnlineStoreTableVersion, OnlineStoreTableVersion, OnlineStoreTableVersionUpdate
    ]
):
    """
    OnlineStoreTableVersionService class

    This service is used to manage the current active version of every aggregation result names in
    the online store tables.
    """

    document_class = OnlineStoreTableVersion
    document_update_class = OnlineStoreTableVersionUpdate

    async def get_version(self, aggregation_result_name: str) -> Optional[int]:
        """
        Get the version of an aggregation result name

        Parameters
        ----------
        aggregation_result_name: str
            Aggregation result name

        Returns
        -------
        Optional[int]
            Version of the OnlineStoreTableVersion
        """
        query_filter = {"aggregation_result_name": aggregation_result_name}
        async for doc in self.list_documents_as_dict_iterator(
            query_filter=query_filter, projection={"version": 1}
        ):
            return int(doc["version"])
        return None

    async def get_versions(self, aggregation_result_names: list[str]) -> Dict[str, int]:
        """
        Get the versions for a list of aggregation result names

        Parameters
        ----------
        aggregation_result_names: list[str]
            List of aggregation result name

        Returns
        -------
        Dict[str, int]
            Versions of the OnlineStoreTableVersion
        """
        query_filter = {"aggregation_result_name": {"$in": aggregation_result_names}}
        out = {}
        async for doc in self.list_documents_as_dict_iterator(
            query_filter=query_filter, projection={"aggregation_result_name": 1, "version": 1}
        ):
            out[doc["aggregation_result_name"]] = doc["version"]
        return out

    async def update_version(self, aggregation_result_name: str, version: int) -> None:
        """
        Update the version of an aggregation result name

        Parameters
        ----------
        aggregation_result_name: str
            Aggregation result name
        version: int
            New version to be associated with the aggregation result name

        Raises
        ------
        DocumentNotFoundError
            If the aggregation result name is not found
        """
        query_filter = {"aggregation_result_name": aggregation_result_name}
        document_id = None
        async for doc in self.list_documents_as_dict_iterator(
            query_filter=query_filter, projection={"_id": 1}
        ):
            document_id = doc["_id"]
            break
        if document_id is None:
            raise DocumentNotFoundError("Aggregation result name not found")
        await self.update_document(document_id, OnlineStoreTableVersionUpdate(version=version))
