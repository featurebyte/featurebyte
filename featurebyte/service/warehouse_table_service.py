"""
WarehouseTableService task
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, AsyncIterator, Optional

from bson import ObjectId

from featurebyte.models.warehouse_table import WarehouseTableModel
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.session.base import BaseSession, QueryMetadata


class WarehouseTableService(
    BaseDocumentService[WarehouseTableModel, WarehouseTableModel, BaseDocumentServiceUpdateSchema]
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
        feature_store_id: ObjectId,
        table_name: str,
        schema_name: Optional[str] = None,
        database_name: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """
        Drop a table using the drop_table method with a session and delete the corresponding
        WarehouseTableModel record

        Parameters
        ----------
        session: BaseSession
            Database session
        feature_store_id: ObjectId
            Feature store ID
        table_name: str
            Table name
        schema_name: Optional[str]
            Schema name
        database_name: Optional[str]
            Database name
        **kwargs: Any
            Additional keyword arguments to be passed to drop_table
        """
        if database_name is None:
            database_name = session.database_name
        if schema_name is None:
            schema_name = session.schema_name
        await session.drop_table(
            table_name=table_name,
            schema_name=schema_name,
            database_name=database_name,
            **kwargs,
        )
        location = TabularSource(
            feature_store_id=feature_store_id,
            table_details=TableDetails(
                table_name=table_name,
                schema_name=schema_name,
                database_name=database_name,
            ),
        )
        warehouse_table = await self.get_warehouse_table_by_location(location)
        if warehouse_table is not None:
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

    async def get_warehouse_table_by_location(
        self, location: TabularSource
    ) -> Optional[WarehouseTableModel]:
        """
        Get a document by location

        Parameters
        ----------
        location: TabularSource
            Location to filter by

        Returns
        -------
        Optional[WarehouseTableModel]
            WarehouseTableModel or None
        """
        query_filter = {"location": location.model_dump()}
        doc = None
        async for doc in self.list_documents_iterator(query_filter=query_filter):
            break
        return doc
