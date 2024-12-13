"""
TimeSeriesTable API route controller
"""

from __future__ import annotations

from bson import ObjectId

from featurebyte.enum import SemanticType
from featurebyte.models.time_series_table import TimeSeriesTableModel
from featurebyte.routes.common.base_table import BaseTableDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.info import TimeSeriesTableInfo
from featurebyte.schema.time_series_table import TimeSeriesTableList, TimeSeriesTableServiceUpdate
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.specialized_dtype import SpecializedDtypeDetectionService
from featurebyte.service.table_columns_info import TableDocumentService
from featurebyte.service.table_facade import TableFacadeService
from featurebyte.service.table_info import TableInfoService
from featurebyte.service.target import TargetService
from featurebyte.service.time_series_table import TimeSeriesTableService


class TimeSeriesTableController(
    BaseTableDocumentController[TimeSeriesTableModel, TimeSeriesTableService, TimeSeriesTableList]
):
    """
    TimeSeriesTable controller
    """

    paginated_document_class = TimeSeriesTableList
    document_update_schema_class = TimeSeriesTableServiceUpdate
    semantic_tag_rules = {
        **BaseTableDocumentController.semantic_tag_rules,
        "series_id_column": SemanticType.SERIES_ID,
        "reference_datetime_column": SemanticType.TIME_SERIES_DATE_TIME,
    }

    def __init__(
        self,
        time_series_table_service: TableDocumentService,
        table_facade_service: TableFacadeService,
        semantic_service: SemanticService,
        entity_service: EntityService,
        feature_service: FeatureService,
        target_service: TargetService,
        feature_list_service: FeatureListService,
        table_info_service: TableInfoService,
        feature_job_setting_analysis_service: FeatureJobSettingAnalysisService,
        specialized_dtype_detection_service: SpecializedDtypeDetectionService,
        feature_store_service: FeatureStoreService,
        feature_store_warehouse_service: FeatureStoreWarehouseService,
        task_controller: TaskController,
    ):
        super().__init__(
            service=time_series_table_service,
            table_facade_service=table_facade_service,
            semantic_service=semantic_service,
            entity_service=entity_service,
            feature_service=feature_service,
            target_service=target_service,
            feature_list_service=feature_list_service,
            specialized_dtype_detection_service=specialized_dtype_detection_service,
            feature_store_service=feature_store_service,
            feature_store_warehouse_service=feature_store_warehouse_service,
            task_controller=task_controller,
        )
        self.table_info_service = table_info_service
        self.feature_job_setting_analysis_service = feature_job_setting_analysis_service

    async def get_info(self, document_id: ObjectId, verbose: bool) -> TimeSeriesTableInfo:
        """
        Get document info given document ID

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Flag to control verbose level

        Returns
        -------
        TimeSeriesTableInfo
        """
        time_series_table = await self.service.get_document(document_id=document_id)
        table_dict = await self.table_info_service.get_table_info(
            data_document=time_series_table, verbose=verbose
        )
        return TimeSeriesTableInfo(
            **table_dict,
            series_id_column=time_series_table.series_id_column,
            reference_datetime_column=time_series_table.reference_datetime_column,
            reference_datetime_schema=time_series_table.reference_datetime_schema,
            time_interval=time_series_table.time_interval,
            default_feature_job_setting=time_series_table.default_feature_job_setting,
        )
