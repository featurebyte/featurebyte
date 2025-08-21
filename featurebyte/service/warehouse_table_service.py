"""
WarehouseTableService task
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, AsyncIterator, Optional

from bson import ObjectId

from featurebyte.models.warehouse_table import WarehouseTableModel, WarehouseTableServiceUpdate
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.session.base import BaseSession, QueryMetadata


class WarehouseTableService(
    BaseDocumentService[WarehouseTableModel, WarehouseTableModel, WarehouseTableServiceUpdate]
):
    """
    WarehouseTableService class
    """

    document_class = WarehouseTableModel

    async def create_table_as_with_session(
        self,
        session: BaseSession,
        feature_store_id: ObjectId,
        table_details: TableDetails | str,
        tag: Optional[str] = None,
        time_to_live_seconds: Optional[int] = None,
        query_metadata: Optional[QueryMetadata] = None,
        **kwargs: Any,
    ) -> WarehouseTableModel:
        """
        Create a table using the create_table_as method with a session and create a
        WarehouseTableModel record

        Parameters
        ----------
        session: BaseSession
            Database session
        feature_store_id: ObjectId
            Feature store ID
        table_details: TableDetails | str
            Table details to be passed to create_table_as
        tag: Optional[str]
            Tag to identify a collection of tables
        time_to_live_seconds: Optional[int]
            Time to live in seconds
        query_metadata: Optional[QueryMetadata]
            Metadata for the query
        **kwargs: Any
            Additional keyword arguments to be passed to create_table_as

        Returns
        -------
        WarehouseTableModel
        """
        await session.create_table_as(
            table_details=table_details, query_metadata=query_metadata, **kwargs
        )
        if isinstance(table_details, str):
            table_details = TableDetails(table_name=table_details)
        if table_details.database_name is None:
            table_details.database_name = session.database_name
        if table_details.schema_name is None:
            table_details.schema_name = session.schema_name
        expires_at = (
            datetime.utcnow() + timedelta(seconds=time_to_live_seconds)
            if time_to_live_seconds is not None
            else None
        )
        model = WarehouseTableModel(
            location=TabularSource(
                feature_store_id=feature_store_id,
                table_details=table_details,
            ),
            tag=str(tag),
            expires_at=expires_at,
        )
        return await self.create_document(model)

    async def drop_table_with_session(
        self,
        session: BaseSession,
        warehouse_table: WarehouseTableModel,
        **kwargs: Any,
    ) -> None:
        """
        Drop a table using the drop_table method with a session and delete the corresponding
        WarehouseTableModel record

        Parameters
        ----------
        session: BaseSession
            Database session
        warehouse_table: WarehouseTableModel
            The warehouse table document to drop
        **kwargs: Any
            Additional keyword arguments to be passed to drop_table
        """
        table_details = warehouse_table.location.table_details
        await session.drop_table(
            table_name=table_details.table_name,
            schema_name=table_details.schema_name or session.schema_name,
            database_name=table_details.database_name or session.database_name,
            **kwargs,
        )
        await self.delete_document(document_id=warehouse_table.id)

    async def list_warehouse_tables_by_tag(self, tag: str) -> AsyncIterator[WarehouseTableModel]:
        """
        List warehouse tables by tag

        Parameters
        ----------
        tag: str
            Tag to filter by

        Yields
        ------
        WarehouseTableModel
            WarehouseTableModel documents matching the provided tag
        """
        query_filter = {"tag": str(tag)}
        async for doc in self.list_documents_iterator(query_filter=query_filter):
            yield doc

    async def list_warehouse_tables_due_for_cleanup(
        self, feature_store_id: ObjectId
    ) -> AsyncIterator[WarehouseTableModel]:
        """
        List warehouse tables that are due for cleanup (expired)

        Since WarehouseTableModel is not catalog-specific, this method naturally
        operates across all catalogs for a given feature store.

        Parameters
        ----------
        feature_store_id: ObjectId
            Feature store ID to filter by

        Yields
        ------
        WarehouseTableModel
            WarehouseTableModel documents that are expired and should be cleaned up
        """
        query_filter = {
            "location.feature_store_id": feature_store_id,
            "expires_at": {"$lt": datetime.utcnow()},
        }
        async for doc in self.list_documents_iterator(query_filter=query_filter):
            yield doc
