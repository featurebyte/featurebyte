"""
TileRegistryService class
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.tile import TileType
from featurebyte.models.tile_registry import (
    BackfillMetadata,
    LastRunMetadata,
    TileModel,
    TileUpdate,
)
from featurebyte.service.base_document import BaseDocumentService


class TileRegistryService(BaseDocumentService[TileModel, TileModel, TileUpdate]):
    """
    TileRegistryService class

    This service is used to keep track of the state of tiles in persistent
    """

    document_class = TileModel
    document_update_class = TileUpdate

    async def get_tile_model(self, tile_id: str, aggregation_id: str) -> Optional[TileModel]:
        """
        Get the tile entry of a tile id

        Parameters
        ----------
        tile_id: str
            Tile id
        aggregation_id: str
            Aggregation id

        Returns
        -------
        Optional[TileModel]
            TileModel if the entry exists
        """
        query_filter = {"tile_id": tile_id, "aggregation_id": aggregation_id}
        async for tile in self.list_documents_iterator(query_filter):
            return tile
        return None

    async def update_last_run_metadata(
        self,
        tile_id: str,
        aggregation_id: str,
        tile_type: TileType,
        tile_index: int,
        tile_end_date: datetime,
    ) -> None:
        """
        Update information about the latest run of tile generation

        Parameters
        ----------
        tile_id: str
            Tile id
        aggregation_id: str
            Aggregation id
        tile_type: TileType
            Tile type (online or offline)
        tile_index: int
            Tile index corresponding to tile_end_date
        tile_end_date: datetime
            Tile end date of the latest tile generation run

        Raises
        ------
        DocumentNotFoundError
            If the tile model is not found
        """
        document = await self.get_tile_model(tile_id, aggregation_id)
        if document is None:
            raise DocumentNotFoundError(
                f"TileRegistryService: TileModel with tile_id={tile_id} and aggregation_id={aggregation_id} not found"
            )
        metadata_model = LastRunMetadata(index=tile_index, tile_end_date=tile_end_date)
        if tile_type == TileType.ONLINE:
            update_model = TileUpdate(last_run_metadata_online=metadata_model)
        else:
            update_model = TileUpdate(last_run_metadata_offline=metadata_model)
        await self.update_document(document.id, update_model, document=document)

    async def update_backfill_metadata(
        self, tile_id: str, aggregation_id: str, backfill_start_date: datetime
    ) -> None:
        """
        Update information about the tile backfill process

        Parameters
        ----------
        tile_id: str
            Tile id
        aggregation_id: str
            Aggregation id
        backfill_start_date: datetime
            Start date of the backfill process

        Raises
        ------
        DocumentNotFoundError
            If the tile model is not found
        """
        document = await self.get_tile_model(tile_id, aggregation_id)
        if document is None:
            raise DocumentNotFoundError(
                f"TileRegistryService: TileModel with tile_id={tile_id} and aggregation_id={aggregation_id} not found"
            )
        metadata_model = BackfillMetadata(start_date=backfill_start_date)
        update_model = TileUpdate(backfill_metadata=metadata_model)
        await self.update_document(document.id, update_model, document=document)
