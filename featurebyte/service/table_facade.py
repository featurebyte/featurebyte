"""
Table  Facade Service which is responsible for handling high level table operations
"""

from typing import List, Optional

from bson import ObjectId

from featurebyte import ColumnCleaningOperation
from featurebyte.enum import TableDataType
from featurebyte.models.feature_store import TableStatus
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.critical_data_info import CriticalDataInfo
from featurebyte.schema.event_table import EventTableServiceUpdate
from featurebyte.schema.feature_job_setting_analysis import FeatureJobSetting
from featurebyte.service.dimension_table import DimensionTableService
from featurebyte.service.event_table import EventTableService
from featurebyte.service.item_table import ItemTableService
from featurebyte.service.scd_table import SCDTableService
from featurebyte.service.table import TableService
from featurebyte.service.table_columns_info import TableColumnsInfoService, TableDocumentService
from featurebyte.service.table_status import TableStatusService


class TableFacadeService:
    """
    Table Facade Service which is responsible for handling high level table operations,
    and delegating the work to the appropriate services.
    """

    def __init__(
        self,
        table_service: TableService,
        event_table_service: EventTableService,
        item_table_service: ItemTableService,
        dimension_table_service: DimensionTableService,
        scd_table_service: SCDTableService,
        table_columns_info_service: TableColumnsInfoService,
        table_status_service: TableStatusService,
    ):
        self.table_service = table_service
        self.event_table_service = event_table_service
        self.item_table_service = item_table_service
        self.dimension_table_service = dimension_table_service
        self.scd_table_service = scd_table_service
        self.table_columns_info_service = table_columns_info_service
        self.table_status_service = table_status_service

    def get_specific_table_service(self, table_type: TableDataType) -> TableDocumentService:
        """
        Get specific table service based on table type

        Parameters
        ----------
        table_type: TableDataType
            Table type

        Returns
        -------
        TableDocumentService
        """
        table_service_map = {
            TableDataType.EVENT_TABLE: self.event_table_service,
            TableDataType.ITEM_TABLE: self.item_table_service,
            TableDataType.DIMENSION_TABLE: self.dimension_table_service,
            TableDataType.SCD_TABLE: self.scd_table_service,
        }
        return table_service_map[table_type]  # type: ignore

    async def update_table_columns_info(
        self,
        table_id: ObjectId,
        columns_info: List[ColumnInfo],
        service: Optional[TableDocumentService] = None,
        skip_block_modification_check: bool = False,
    ) -> None:
        """
        Update table columns info

        Parameters
        ----------
        table_id: ObjectId
            Table id
        columns_info: List[ColumnInfo]
            Columns info
        service: Optional[TableDocumentService]
            Table document service
        skip_block_modification_check: bool
            Flag to skip block modification check (used only when updating table column description)
        """
        if service is None:
            table = await self.table_service.get_document(table_id)
            service = self.get_specific_table_service(table.type)
        await self.table_columns_info_service.update_columns_info(
            service=service,
            document_id=table_id,
            columns_info=columns_info,
            skip_block_modification_check=skip_block_modification_check,
        )

    async def update_table_column_cleaning_operations(
        self,
        table_id: ObjectId,
        column_cleaning_operations: List[ColumnCleaningOperation],
        service: Optional[TableDocumentService] = None,
    ) -> None:
        """
        Update table column cleaning operations

        Parameters
        ----------
        table_id: ObjectId
            Table id
        column_cleaning_operations: List[ColumnCleaningOperation]
            Column cleaning operations
        service: Optional[TableDocumentService]
            Table document service

        Raises
        ------
        ValueError
            If column not found in table
        """
        if service is None:
            table = await self.table_service.get_document(table_id)
            service = self.get_specific_table_service(table.type)
        else:
            table = await service.get_document(table_id)

        columns_info = table.columns_info
        col_name_to_cleaning_operations = {
            col_clean_op.column_name: col_clean_op.cleaning_operations
            for col_clean_op in column_cleaning_operations
        }
        for col_info in columns_info:
            if col_info.name in col_name_to_cleaning_operations:
                col_info.critical_data_info = CriticalDataInfo(
                    cleaning_operations=col_name_to_cleaning_operations.pop(col_info.name)
                )

        if col_name_to_cleaning_operations:
            raise ValueError(
                f"Columns {list(col_name_to_cleaning_operations.keys())} not found in table {table_id}"
            )

        await self.table_columns_info_service.update_columns_info(
            service=service, document_id=table_id, columns_info=columns_info
        )

    async def update_table_status(
        self,
        table_id: ObjectId,
        status: TableStatus,
        service: Optional[TableDocumentService] = None,
    ) -> None:
        """
        Update table status

        Parameters
        ----------
        table_id: ObjectId
            Table id
        status: TableStatus
            Table status
        service: Optional[TableDocumentService]
            Table document service
        """
        if service is None:
            table = await self.table_service.get_document(table_id)
            service = self.get_specific_table_service(table.type)
        await self.table_status_service.update_status(
            service=service, document_id=table_id, status=status
        )

    async def update_default_feature_job_setting(
        self, table_id: ObjectId, default_feature_job_setting: FeatureJobSetting
    ) -> None:
        """
        Update default feature job setting

        Parameters
        ----------
        table_id: ObjectId
            Table id
        default_feature_job_setting: FeatureJobSetting
            Default feature job setting
        """
        await self.event_table_service.update_document(
            document_id=table_id,
            data=EventTableServiceUpdate(default_feature_job_setting=default_feature_job_setting),
            return_document=False,
        )
