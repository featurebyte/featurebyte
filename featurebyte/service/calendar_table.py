"""
CalendarTableService class
"""

from __future__ import annotations

from featurebyte.exception import DocumentCreationError
from featurebyte.models.calendar_table import CalendarTableModel
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.schema.calendar_table import CalendarTableCreate, CalendarTableServiceUpdate
from featurebyte.service.base_table_document import BaseTableDocumentService


class CalendarTableService(
    BaseTableDocumentService[CalendarTableModel, CalendarTableCreate, CalendarTableServiceUpdate]
):
    """
    CalendarTableService class
    """

    document_class = CalendarTableModel
    document_update_class = CalendarTableServiceUpdate

    @property
    def class_name(self) -> str:
        return "CalendarTable"

    async def create_document(self, data: CalendarTableCreate) -> CalendarTableModel:
        # retrieve feature store to check the feature_store_id is valid
        feature_store = await self.feature_store_service.get_document(
            document_id=data.tabular_source.feature_store_id
        )
        # check whether the document has time schema in the columns with string type
        sql_adapter = get_sql_adapter(source_info=feature_store.get_source_info())

        # if calendar datetime column has a format string ensure it does not contain timezone information
        if data.calendar_datetime_schema.format_string is not None:
            if sql_adapter.format_string_has_timezone(data.calendar_datetime_schema.format_string):
                raise DocumentCreationError(
                    "Timezone information in calendar_datetime_column is not supported for CalendarTable."
                )

        return await super().create_document(data=data)
