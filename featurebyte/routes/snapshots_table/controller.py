"""
SnapshotsTable API route controller
"""

from __future__ import annotations

from bson import ObjectId

from featurebyte.enum import SemanticType
from featurebyte.models.snapshots_table import SnapshotsTableModel
from featurebyte.routes.common.base_table import BaseTableDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.info import SnapshotsTableInfo
from featurebyte.schema.snapshots_table import SnapshotsTableList, SnapshotsTableServiceUpdate
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.snapshots_table import SnapshotsTableService
from featurebyte.service.specialized_dtype import SpecializedDtypeDetectionService
from featurebyte.service.table_columns_info import TableDocumentService
from featurebyte.service.table_facade import TableFacadeService
from featurebyte.service.table_info import TableInfoService
from featurebyte.service.target import TargetService


class SnapshotsTableController(
    BaseTableDocumentController[SnapshotsTableModel, SnapshotsTableService, SnapshotsTableList]
):
    """
    SnapshotsTable controller
    """

    paginated_document_class = SnapshotsTableList
    document_update_schema_class = SnapshotsTableServiceUpdate
    semantic_tag_rules = {
        **BaseTableDocumentController.semantic_tag_rules,
        "snapshot_id_column": SemanticType.SNAPSHOT_ID,
        "snapshot_datetime_column": SemanticType.SNAPSHOT_DATE_TIME,
    }

    def __init__(
        self,
        snapshots_table_service: TableDocumentService,
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
            service=snapshots_table_service,
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

    async def get_info(self, document_id: ObjectId, verbose: bool) -> SnapshotsTableInfo:
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
        SnapshotsTableInfo
        """
        snapshots_table = await self.service.get_document(document_id=document_id)
        table_dict = await self.table_info_service.get_table_info(
            data_document=snapshots_table, verbose=verbose
        )
        return SnapshotsTableInfo(
            **table_dict,
            snapshot_id_column=snapshots_table.snapshot_id_column,
            snapshot_datetime_column=snapshots_table.snapshot_datetime_column,
            snapshot_datetime_schema=snapshots_table.snapshot_datetime_schema,
            time_interval=snapshots_table.time_interval,
            default_feature_job_setting=snapshots_table.default_feature_job_setting,
        )
