"""
DeployedTileTableService class
"""

from __future__ import annotations

from typing import AsyncIterator

from featurebyte.models.deployed_tile_table import DeployedTileTableModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.service.base_document import BaseDocumentService


class DeployedTileTableService(
    BaseDocumentService[
        DeployedTileTableModel, DeployedTileTableModel, BaseDocumentServiceUpdateSchema
    ]
):
    """
    DeployedTileTableService class
    """

    document_class = DeployedTileTableModel

    async def get_deployed_aggregation_ids(self, aggregation_ids: set[str]) -> set[str]:
        """
        Get aggregation_ids that are associated with deployed tile tables

        Parameters
        ----------
        aggregation_ids: set[str]
            Set of aggregation_ids to check

        Returns
        -------
        set[str]
            Set of aggregation_ids that are associated with deployed tile tables
        """
        deployed_aggregation_ids = set()
        async for doc in self.list_deployed_tile_tables_by_aggregation_ids(aggregation_ids):
            doc_aggregation_ids = [
                tile_identifier.aggregation_id for tile_identifier in doc.tile_identifiers
            ]
            deployed_aggregation_ids.update(doc_aggregation_ids)
        return deployed_aggregation_ids.intersection(aggregation_ids)

    async def list_deployed_tile_tables_by_aggregation_ids(
        self, aggregation_ids: set[str]
    ) -> AsyncIterator[DeployedTileTableModel]:
        """
        List deployed tile tables by aggregation_ids

        Parameters
        ----------
        aggregation_ids: set[str]
            Set of aggregation_ids to check

        Yields
        ------
        AsyncIterator[DeployedTileTableModel]
            Deployed tile table document
        """
        async for doc in self.list_documents_iterator(
            query_filter={"tile_identifiers.aggregation_id": {"$in": list(aggregation_ids)}}
        ):
            yield doc
