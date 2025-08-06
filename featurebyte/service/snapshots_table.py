"""
SnapshotsTableService class
"""

from __future__ import annotations

from typing import Optional

from bson import ObjectId
from pydantic_extra_types.timezone_name import TimeZoneName

from featurebyte.exception import DocumentCreationError, DocumentUpdateError
from featurebyte.models.snapshots_table import SnapshotsTableModel
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.schema.snapshots_table import SnapshotsTableCreate, SnapshotsTableServiceUpdate
from featurebyte.service.base_table_document import BaseTableDocumentService


class SnapshotsTableService(
    BaseTableDocumentService[SnapshotsTableModel, SnapshotsTableCreate, SnapshotsTableServiceUpdate]
):
    """
    SnapshotsTableService class
    """

    document_class = SnapshotsTableModel
    document_update_class = SnapshotsTableServiceUpdate

    @property
    def class_name(self) -> str:
        return "SnapshotsTable"

    async def create_document(self, data: SnapshotsTableCreate) -> SnapshotsTableModel:
        # retrieve feature store to check the feature_store_id is valid
        feature_store = await self.feature_store_service.get_document(
            document_id=data.tabular_source.feature_store_id
        )
        # check whether the document has time schema in the columns with string type
        sql_adapter = get_sql_adapter(source_info=feature_store.get_source_info())

        # if snapshot datetime column has a format string ensure it does not contain timezone information
        if data.snapshot_datetime_schema.format_string is not None:
            if sql_adapter.format_string_has_timezone(data.snapshot_datetime_schema.format_string):
                raise DocumentCreationError(
                    "Timezone information in snapshot_datetime_column is not supported for SnapshotsTable."
                )

        if data.snapshot_datetime_schema.timezone_offset_column_name is not None:
            raise DocumentCreationError(
                "Timezone column in snapshot_datetime_schema is not supported for SnapshotsTable."
            )

        return await super().create_document(data=data)

    async def update_document(
        self,
        document_id: ObjectId,
        data: SnapshotsTableServiceUpdate,
        exclude_none: bool = True,
        document: Optional[SnapshotsTableModel] = None,
        return_document: bool = True,
        skip_block_modification_check: bool = False,
        populate_remote_attributes: bool = True,
    ) -> Optional[SnapshotsTableModel]:
        if isinstance(data, SnapshotsTableServiceUpdate) and data.default_feature_job_setting:
            # if default feature job setting is to be updated
            document = await self.get_document(document_id=document_id)
            snapshot_datetime_col_info = next(
                col_info
                for col_info in document.columns_info
                if col_info.name == document.snapshot_datetime_column
            )
            timezone = (
                snapshot_datetime_col_info.timestamp_schema
                and snapshot_datetime_col_info.timestamp_schema.timezone
            )
            update_to_timezone = data.default_feature_job_setting.reference_timezone
            if isinstance(timezone, TimeZoneName):
                if update_to_timezone and update_to_timezone != timezone:
                    raise DocumentUpdateError(
                        f"Cannot update default feature job setting reference timezone to {update_to_timezone} "
                        f"as it is different from the timezone of the reference datetime column ({timezone})."
                    )
                else:
                    # update the timezone to the timezone of the reference datetime column if not provided
                    data.default_feature_job_setting.reference_timezone = timezone

        return await super().update_document(
            document_id=document_id,
            data=data,
            exclude_none=exclude_none,
            document=document,
            return_document=return_document,
            skip_block_modification_check=skip_block_modification_check,
            populate_remote_attributes=populate_remote_attributes,
        )
