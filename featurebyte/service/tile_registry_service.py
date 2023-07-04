"""
TileRegistryService class
"""
from __future__ import annotations

from typing import Optional

from datetime import datetime

from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.tile import TileType
from featurebyte.models.tile_registry import TileModel, TileUpdate
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
        async for doc in self.list_documents_iterator(query_filter):
            return TileModel(**doc)
        return None

    async def update_last_tile_info(
        self,
        tile_id: str,
        aggregation_id: str,
        tile_type: TileType,
        tile_index: int,
        tile_start_date: datetime,
    ) -> None:
        """
        Update information about the last tile

        Parameters
        ----------
        tile_id: str
            Tile id
        aggregation_id: str
            Aggregation id
        tile_type: TileType
            Tile type (online or offline)
        tile_index: int
            Index of the last tile
        tile_start_date: datetime
            Start date of the last tile

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
        if tile_type == TileType.ONLINE:
            update_model = TileUpdate(
                last_tile_index_online=tile_index,
                last_tile_start_date_online=tile_start_date,
            )
        else:
            update_model = TileUpdate(
                last_tile_index_offline=tile_index,
                last_tile_start_date_offline=tile_start_date,
            )
        await self.update_document(document.id, update_model, document=document)
