"""
DeployedTileTableService class
"""

from __future__ import annotations

from datetime import datetime
from typing import AsyncIterator, Optional

from bson import ObjectId

from featurebyte.models.deployed_tile_table import DeployedTileTableInfo, DeployedTileTableModel
from featurebyte.models.tile import TileType
from featurebyte.models.tile_registry import BackfillMetadata, LastRunMetadata, TileUpdate
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

    async def get_deployed_tile_table_info(
        self, aggregation_ids: Optional[set[str]] = None
    ) -> DeployedTileTableInfo:
        """
        Get deployed tile table information

        Parameters
        ----------
        aggregation_ids: Optional[set[str]]
            Set of aggregation_ids to filter the deployed tile tables

        Returns
        -------
        DeployedTileTableInfo
            Deployed tile table information
        """
        deployed_tile_tables = []
        if aggregation_ids is not None:
            docs = self.list_deployed_tile_tables_by_aggregation_ids(aggregation_ids)
        else:
            docs = self.list_documents_iterator(query_filter={})
        async for doc in docs:
            deployed_tile_tables.append(doc)
        return DeployedTileTableInfo(deployed_tile_tables=deployed_tile_tables)

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

    async def update_last_run_metadata(
        self,
        document_id: ObjectId,
        tile_type: TileType,
        tile_index: int,
        tile_end_date: datetime,
    ) -> None:
        """
        Update information about the latest run of tile generation

        Parameters
        ----------
        document_id: ObjectId
            Document ID of the tile model
        tile_type: TileType
            Tile type (online or offline)
        tile_index: int
            Tile index corresponding to tile_end_date
        tile_end_date: datetime
            Tile end date of the latest tile generation run
        """
        document = await self.get_document(document_id)
        metadata_model = LastRunMetadata(index=tile_index, tile_end_date=tile_end_date)
        if tile_type == TileType.ONLINE:
            update_model = TileUpdate(last_run_metadata_online=metadata_model)
        else:
            update_model = TileUpdate(last_run_metadata_offline=metadata_model)
        await self.update_document(document.id, update_model, document=document)

    async def update_backfill_metadata(
        self, document_id: ObjectId, backfill_start_date: datetime
    ) -> None:
        """
        Update information about the tile backfill process

        Parameters
        ----------
        document_id: ObjectId
            Document ID of the tile model
        backfill_start_date: datetime
            Start date of the backfill process
        """
        document = await self.get_document(document_id)
        metadata_model = BackfillMetadata(start_date=backfill_start_date)
        update_model = TileUpdate(backfill_metadata=metadata_model)
        await self.update_document(document.id, update_model, document=document)
