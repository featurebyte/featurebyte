"""
ItemTableService class
"""

from __future__ import annotations

from typing import Any, Optional

from bson import ObjectId
from redis import Redis

from featurebyte.exception import DocumentCreationError
from featurebyte.models.item_table import ItemTableModel
from featurebyte.persistent import Persistent
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.item_table import ItemTableCreate, ItemTableServiceUpdate
from featurebyte.service.base_table_document import BaseTableDocumentService
from featurebyte.service.event_table import EventTableService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.storage import Storage


class ItemTableService(
    BaseTableDocumentService[ItemTableModel, ItemTableCreate, ItemTableServiceUpdate]
):
    """
    ItemTableService class
    """

    document_class = ItemTableModel
    document_update_class = ItemTableServiceUpdate

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        block_modification_handler: BlockModificationHandler,
        feature_store_service: FeatureStoreService,
        storage: Storage,
        redis: Redis[Any],
        event_table_service: EventTableService,
    ):
        super().__init__(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            block_modification_handler=block_modification_handler,
            feature_store_service=feature_store_service,
            storage=storage,
            redis=redis,
        )
        self.event_table_service = event_table_service

    @property
    def class_name(self) -> str:
        return "ItemTable"

    async def create_document(self, data: ItemTableCreate) -> ItemTableModel:
        # validate event ID column in the event table has matching data type with that in the item table
        item_table_event_id_dtype = [
            column_info
            for column_info in data.columns_info
            if column_info.name == data.event_id_column
        ][0].dtype
        event_table_model = await self.event_table_service.get_document(
            document_id=data.event_table_id
        )

        if event_table_model.event_id_column is None:
            raise DocumentCreationError(
                f"Event ID column not available in event table {event_table_model.name} (ID: {event_table_model.id})."
            )

        event_table_event_id_dtype = [
            column_info
            for column_info in event_table_model.columns_info
            if column_info.name == event_table_model.event_id_column
        ][0].dtype
        if item_table_event_id_dtype != event_table_event_id_dtype:
            raise DocumentCreationError(
                f"Data type mismatch between event ID columns of event table {event_table_model.name} "
                f"(ID: {event_table_model.id}) ({event_table_event_id_dtype}) and "
                f"item table ({item_table_event_id_dtype})."
            )
        return await super().create_document(data=data)


class ExtendedItemTableService:
    """
    ExtendedItemTableService class
    """

    def __init__(self, item_table_service: ItemTableService):
        self.item_table_service = item_table_service

    async def get_document_with_event_table_model(self, document_id: ObjectId) -> ItemTableModel:
        """
        Get ItemTableModel with the event_table_model field populated

        Parameters
        ----------
        document_id: ObjectId
            Document ID

        Returns
        -------
        ItemTableModel
        """
        item_table = await self.item_table_service.get_document(document_id=document_id)
        item_table.event_table_model = (
            await self.item_table_service.event_table_service.get_document(
                document_id=item_table.event_table_id
            )
        )
        return item_table
