"""
TimeSeriesTableService class
"""

from __future__ import annotations

from featurebyte.exception import DocumentCreationError
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
