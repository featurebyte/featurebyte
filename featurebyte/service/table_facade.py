"""
Table  Facade Service which is responsible for handling high level table operations
"""

from typing import List, Optional, Union

from bson import ObjectId

from featurebyte import ColumnCleaningOperation
from featurebyte.enum import TableDataType
from featurebyte.models.feature_store import TableModel, TableStatus
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.critical_data_info import CriticalDataInfo
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSettingUnion
from featurebyte.schema.event_table import EventTableServiceUpdate
from featurebyte.schema.scd_table import SCDTableServiceUpdate
from featurebyte.schema.time_series_table import TimeSeriesTableServiceUpdate
from featurebyte.service.dimension_table import DimensionTableService
from featurebyte.service.dimension_table_validation import DimensionTableValidationService
from featurebyte.service.event_table import EventTableService
from featurebyte.service.event_table_validation import EventTableValidationService
from featurebyte.service.item_table import ItemTableService
from featurebyte.service.item_table_validation import ItemTableValidationService
from featurebyte.service.scd_table import SCDTableService
from featurebyte.service.scd_table_validation import SCDTableValidationService
from featurebyte.service.table import TableService
from featurebyte.service.table_columns_info import TableColumnsInfoService, TableDocumentService
from featurebyte.service.table_status import TableStatusService
from featurebyte.service.time_series_table import TimeSeriesTableService
from featurebyte.service.time_series_table_validation import TimeSeriesTableValidationService

TableValidationService = Union[
    EventTableValidationService,
    ItemTableValidationService,
    DimensionTableValidationService,
    SCDTableValidationService,
    TimeSeriesTableValidationService,
]


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
        time_series_table_service: TimeSeriesTableService,
        event_table_validation_service: EventTableValidationService,
        item_table_validation_service: ItemTableValidationService,
        dimension_table_validation_service: DimensionTableValidationService,
        scd_table_validation_service: SCDTableValidationService,
        time_series_table_validation_service: TimeSeriesTableValidationService,
        table_columns_info_service: TableColumnsInfoService,
        table_status_service: TableStatusService,
    ):
        self.table_service = table_service
        self.event_table_service = event_table_service
        self.item_table_service = item_table_service
        self.dimension_table_service = dimension_table_service
        self.scd_table_service = scd_table_service
        self.time_series_table_service = time_series_table_service
        self.event_table_validation_service = event_table_validation_service
        self.item_table_validation_service = item_table_validation_service
        self.dimension_table_validation_service = dimension_table_validation_service
        self.scd_table_validation_service = scd_table_validation_service
        self.time_series_table_validation_service = time_series_table_validation_service
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
            TableDataType.TIME_SERIES_TABLE: self.time_series_table_service,
        }
        return table_service_map[table_type]  # type: ignore

    def get_specific_table_validation_service(
        self, table_type: TableDataType
    ) -> TableValidationService:
        """
        Get specific table validation service based on table type

        Parameters
        ----------
        table_type: TableDataType
            Table type

        Returns
        -------
        TableDocumentService
        """
        table_service_map = {
            TableDataType.EVENT_TABLE: self.event_table_validation_service,
            TableDataType.ITEM_TABLE: self.item_table_validation_service,
            TableDataType.DIMENSION_TABLE: self.dimension_table_validation_service,
            TableDataType.SCD_TABLE: self.scd_table_validation_service,
            TableDataType.TIME_SERIES_TABLE: self.time_series_table_validation_service,
        }
        return table_service_map[table_type]  # type: ignore

    async def update_table_columns_info(
        self,
        table_id: ObjectId,
        columns_info: List[ColumnInfo],
        service: Optional[TableDocumentService] = None,
        skip_semantic_check: bool = False,
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
        skip_semantic_check: bool
            Flag to skip semantic check
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
            skip_semantic_check=skip_semantic_check,
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
        if status == TableStatus.DEPRECATED:
            # remove entity tagging from deprecated table
            table = await service.get_document(table_id)
            columns_info = []
            for col_info in table.columns_info:
                if col_info.entity_id:
                    col_info.entity_id = None
                columns_info.append(col_info)
            await self.update_table_columns_info(table_id, columns_info, service=service)

    async def update_default_feature_job_setting(
        self, table_id: ObjectId, default_feature_job_setting: FeatureJobSettingUnion
    ) -> None:
        """
        Update default feature job setting

        Parameters
        ----------
        table_id: ObjectId
            Table id
        default_feature_job_setting: FeatureJobSettingUnion
            Default feature job setting

        Raises
        ------
        ValueError
            If default feature job setting is not supported for table type
        """
        table = await self.table_service.get_document(table_id)
        if table.type == TableDataType.EVENT_TABLE:
            await self.event_table_service.update_document(
                document_id=table_id,
                data=EventTableServiceUpdate(
                    default_feature_job_setting=default_feature_job_setting
                ),
                return_document=False,
            )
        elif table.type == TableDataType.SCD_TABLE:
            await self.scd_table_service.update_document(
                document_id=table_id,
                data=SCDTableServiceUpdate(default_feature_job_setting=default_feature_job_setting),
                return_document=False,
            )
        elif table.type == TableDataType.TIME_SERIES_TABLE:
            await self.time_series_table_service.update_document(
                document_id=table_id,
                data=TimeSeriesTableServiceUpdate(
                    default_feature_job_setting=default_feature_job_setting
                ),
                return_document=False,
            )
        else:
            raise ValueError(
                f"Default feature job setting not supported for table type {table.type}"
            )

    def table_needs_validation(self, table_model: TableModel) -> bool:
        """
        Check if a table needs validation

        Parameters
        ----------
        table_model: TableModel
            Table model

        Returns
        -------
        bool
        """
        service = self.get_specific_table_validation_service(table_model.type)
        return service.table_needs_validation(table_model)  # type: ignore[arg-type]
