"""
TimeSeriesTableService class
"""

from __future__ import annotations

from typing import Optional

from bson import ObjectId
from pydantic_extra_types.timezone_name import TimeZoneName

from featurebyte.exception import DocumentCreationError, DocumentUpdateError
from featurebyte.models.time_series_table import TimeSeriesTableModel
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.schema.time_series_table import TimeSeriesTableCreate, TimeSeriesTableServiceUpdate
from featurebyte.service.base_table_document import BaseTableDocumentService


class TimeSeriesTableService(
    BaseTableDocumentService[
        TimeSeriesTableModel, TimeSeriesTableCreate, TimeSeriesTableServiceUpdate
    ]
):
    """
    TimeSeriesTableService class
    """

    document_class = TimeSeriesTableModel
    document_update_class = TimeSeriesTableServiceUpdate

    @property
    def class_name(self) -> str:
        return "TimeSeriesTable"

    async def create_document(self, data: TimeSeriesTableCreate) -> TimeSeriesTableModel:
        # retrieve feature store to check the feature_store_id is valid
        feature_store = await self.feature_store_service.get_document(
            document_id=data.tabular_source.feature_store_id
        )
        # check whether the document has time schema in the columns with string type
        sql_adapter = get_sql_adapter(source_info=feature_store.get_source_info())

        # if reference datetime column has a format string ensure it does not contain timezone information
        if data.reference_datetime_schema.format_string is not None:
            if sql_adapter.format_string_has_timezone(data.reference_datetime_schema.format_string):
                raise DocumentCreationError(
                    "Timezone information in time series table reference datetime column is not supported."
                )

        return await super().create_document(data=data)

    async def update_document(
        self,
        document_id: ObjectId,
        data: TimeSeriesTableServiceUpdate,
        exclude_none: bool = True,
        document: Optional[TimeSeriesTableModel] = None,
        return_document: bool = True,
        skip_block_modification_check: bool = False,
        populate_remote_attributes: bool = True,
    ) -> Optional[TimeSeriesTableModel]:
        if isinstance(data, TimeSeriesTableServiceUpdate) and data.default_feature_job_setting:
            # if default feature job setting is to be updated
            document = await self.get_document(document_id=document_id)
            reference_datetime_col_info = next(
                col_info
                for col_info in document.columns_info
                if col_info.name == document.reference_datetime_column
            )
            timezone = (
                reference_datetime_col_info.timestamp_schema
                and reference_datetime_col_info.timestamp_schema.timezone
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
