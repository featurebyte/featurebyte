"""
Base class for materialized table routes
"""

from io import BytesIO
from typing import Any, AsyncGenerator, Optional, TypeVar

import pyarrow as pa
from bson import ObjectId
from pyarrow import parquet as pq
from starlette.responses import StreamingResponse

from featurebyte.common.env_util import is_development_mode
from featurebyte.enum import InternalName
from featurebyte.exception import DocumentDeletionError
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.models.historical_feature_table import HistoricalFeatureTableModel
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.static_source_table import StaticSourceTableModel
from featurebyte.models.target_table import TargetTableModel
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.routes.common.base import BaseDocumentController, PaginatedDocument
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.task import Task
from featurebyte.service.batch_feature_table import BatchFeatureTableService
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.static_source_table import StaticSourceTableService
from featurebyte.service.target_table import TargetTableService

MaterializedTableDocumentT = TypeVar(
    "MaterializedTableDocumentT",
    ObservationTableModel,
    HistoricalFeatureTableModel,
    BatchRequestTableModel,
    BatchFeatureTableModel,
    StaticSourceTableModel,
    TargetTableModel,
)
MaterializedTableDocumentServiceT = TypeVar(
    "MaterializedTableDocumentServiceT",
    ObservationTableService,
    HistoricalFeatureTableService,
    BatchRequestTableService,
    BatchFeatureTableService,
    StaticSourceTableService,
    TargetTableService,
)


class BaseMaterializedTableController(
    BaseDocumentController[
        MaterializedTableDocumentT, MaterializedTableDocumentServiceT, PaginatedDocument
    ]
):
    """
    Base class for materialized table routes
    """

    task_controller: TaskController
    has_internal_row_index_column_in_table: bool = True

    def __init__(
        self, service: Any, feature_store_warehouse_service: FeatureStoreWarehouseService
    ) -> None:
        super().__init__(service)
        self.feature_store_warehouse_service = feature_store_warehouse_service

    async def delete_materialized_table(self, document_id: ObjectId) -> Task:
        """
        Delete materialized table

        Parameters
        ----------
        document_id: ObjectId
            ID of materialized table to delete
        """
        # check if document exists
        _ = await self.service.get_document(document_id=document_id)

        # check if document is used by any other documents
        await self.verify_operation_by_checking_reference(
            document_id=document_id, exception_class=DocumentDeletionError
        )

        # create task payload & submit task
        payload = await self.service.get_materialized_table_delete_task_payload(
            document_id=document_id
        )
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))

    async def download_materialized_table(
        self,
        document_id: ObjectId,
    ) -> StreamingResponse:
        """
        Download materialized table as pyarrow table

        Parameters
        ----------
        document_id: ObjectId
            ID of materialized table to download

        Returns
        -------
        StreamingResponse
            StreamingResponse object
        """
        table = await self.service.get_document(document_id=document_id)
        bytestream = await self.feature_store_warehouse_service.download_table(
            location=table.location,
        )
        assert bytestream is not None

        return StreamingResponse(
            bytestream,
            media_type="application/octet-stream",
        )

    async def download_materialized_table_as_parquet_bytes(
        self,
        location: TabularSource,
    ) -> AsyncGenerator[bytes, None]:
        """
        Download materialized table as parquet bytes generator

        Parameters
        ----------
        location: TabularSource
            ID of materialized table to download

        Returns
        -------
        AsyncGenerator[bytes, None]
        """
        bytestream = await self.feature_store_warehouse_service.download_table(
            location=location,
        )
        assert bytestream is not None

        in_buffer = BytesIO()
        out_buffer = BytesIO()
        reader: Optional[pa.RecordBatchStreamReader] = None
        writer: Optional[pq.ParquetWriter] = None
        async for in_chunk in bytestream:
            in_buffer.write(in_chunk)
            in_buffer.seek(0)
            if not reader:
                reader = pa.ipc.open_stream(in_buffer)
            table = reader.read_all()
            in_buffer.seek(0)
            in_buffer.truncate(0)

            if not writer:
                writer = pq.ParquetWriter(out_buffer, table.schema)
            writer.write_table(table)
            out_buffer.seek(0)
            out_chunk = out_buffer.getvalue()
            if out_chunk:
                yield out_chunk
                out_buffer.seek(0)
                out_buffer.truncate(0)

        # read final chunk
        if reader:
            reader.close()
            if writer:
                writer.write_table(reader.read_all())
                writer.close()
        out_chunk = out_buffer.getvalue()
        if out_chunk:
            yield out_chunk

    async def download_materialized_table_as_parquet(
        self,
        document_id: ObjectId,
    ) -> StreamingResponse:
        """
        Download materialized table as parquet file

        Parameters
        ----------
        document_id: ObjectId
            ID of materialized table to download

        Returns
        -------
        StreamingResponse
            StreamingResponse object
        """
        table = await self.service.get_document(document_id=document_id)
        bytestream = self.download_materialized_table_as_parquet_bytes(
            location=table.location,
        )
        return StreamingResponse(
            bytestream,
            media_type="application/octet-stream",
            headers={
                "content-disposition": (
                    'attachment; name="data"; '
                    f'filename="{table.collection_name()}_{table.name}.parquet"'
                )
            },
        )

    async def preview_materialized_table(
        self,
        document_id: ObjectId,
        limit: int,
        column_names: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        """
        Preview materialized table as pyarrow table

        Parameters
        ----------
        document_id: ObjectId
            ID of materialized table to preview
        limit: int
            Number of rows to preview
        column_names: Optional[list[str]]
            Column names to preview

        Returns
        -------
        dict[str, Any]
            Preview of materialized table
        """
        table = await self.service.get_document(document_id=document_id)
        if self.has_internal_row_index_column_in_table:
            try:
                return await self.feature_store_warehouse_service.table_preview(
                    location=table.location,
                    limit=limit,
                    order_by_column=InternalName.TABLE_ROW_INDEX,
                    column_names=column_names,
                )
            except Exception as exc:
                if is_development_mode():
                    raise exc

                # if table does not have row index column, preview without ordering
                return await self.feature_store_warehouse_service.table_preview(
                    location=table.location, limit=limit, column_names=column_names
                )

        # the table does not have internal row index column
        return await self.feature_store_warehouse_service.table_preview(
            location=table.location, limit=limit, column_names=column_names
        )
