"""
Task registry
"""

from enum import Enum
from typing import Dict, Type

from featurebyte.enum import WorkerCommand
from featurebyte.worker.task.base import BaseTask, TaskT
from featurebyte.worker.task.batch_feature_create import BatchFeatureCreateTask
from featurebyte.worker.task.batch_feature_table import BatchFeatureTableTask
from featurebyte.worker.task.batch_request_table import BatchRequestTableTask
from featurebyte.worker.task.catalog_cleanup import CatalogCleanupTask
from featurebyte.worker.task.catalog_online_store_update import CatalogOnlineStoreUpdateTask
from featurebyte.worker.task.data_description import DataDescriptionTask
from featurebyte.worker.task.deployment_create_update import DeploymentCreateUpdateTask
from featurebyte.worker.task.development_dataset import (
    DevelopmentDatasetAddTablesTask,
    DevelopmentDatasetCreateTask,
    DevelopmentDatasetDeleteTask,
)
from featurebyte.worker.task.feature_job_setting_analysis import FeatureJobSettingAnalysisTask
from featurebyte.worker.task.feature_job_setting_analysis_backtest import (
    FeatureJobSettingAnalysisBacktestTask,
)
from featurebyte.worker.task.feature_list_batch_feature_create import (
    FeatureListCreateWithBatchFeatureCreationTask,
)
from featurebyte.worker.task.feature_list_create import FeatureListCreateTask
from featurebyte.worker.task.feature_list_make_production_ready import (
    FeatureListMakeProductionReadyTask,
)
from featurebyte.worker.task.feature_materialize_sync import FeatureMaterializeSyncTask
from featurebyte.worker.task.historical_feature_table import HistoricalFeatureTableTask
from featurebyte.worker.task.materialized_table_delete import MaterializedTableDeleteTask
from featurebyte.worker.task.observation_table import ObservationTableTask
from featurebyte.worker.task.observation_table_upload import ObservationTableUploadTask
from featurebyte.worker.task.online_store_cleanup import OnlineStoreCleanupTask
from featurebyte.worker.task.query_cache_cleanup import QueryCacheCleanupTask
from featurebyte.worker.task.scheduled_feature_materialize import ScheduledFeatureMaterializeTask
from featurebyte.worker.task.static_source_table import StaticSourceTableTask
from featurebyte.worker.task.table_validation import TableValidationTask
from featurebyte.worker.task.target_table import TargetTableTask
from featurebyte.worker.task.test_task import TestIOTask, TestTask
from featurebyte.worker.task.tile_task import TileTask

# TASK_REGISTRY_MAP contains a mapping of the worker command to the task.
TASK_REGISTRY_MAP: Dict[Enum, Type[BaseTask[TaskT]]] = {  # type: ignore[valid-type]
    WorkerCommand.FEATURE_JOB_SETTING_ANALYSIS_CREATE: FeatureJobSettingAnalysisTask,
    WorkerCommand.FEATURE_JOB_SETTING_ANALYSIS_BACKTEST: FeatureJobSettingAnalysisBacktestTask,
    WorkerCommand.HISTORICAL_FEATURE_TABLE_CREATE: HistoricalFeatureTableTask,
    WorkerCommand.OBSERVATION_TABLE_CREATE: ObservationTableTask,
    WorkerCommand.OBSERVATION_TABLE_UPLOAD: ObservationTableUploadTask,
    WorkerCommand.DEPLOYMENT_CREATE_UPDATE: DeploymentCreateUpdateTask,
    WorkerCommand.BATCH_REQUEST_TABLE_CREATE: BatchRequestTableTask,
    WorkerCommand.BATCH_FEATURE_TABLE_CREATE: BatchFeatureTableTask,
    WorkerCommand.MATERIALIZED_TABLE_DELETE: MaterializedTableDeleteTask,
    WorkerCommand.BATCH_FEATURE_CREATE: BatchFeatureCreateTask,
    WorkerCommand.FEATURE_LIST_CREATE: FeatureListCreateTask,
    WorkerCommand.FEATURE_LIST_MAKE_PRODUCTION_READY: FeatureListMakeProductionReadyTask,
    WorkerCommand.STATIC_SOURCE_TABLE_CREATE: StaticSourceTableTask,
    WorkerCommand.TARGET_TABLE_CREATE: TargetTableTask,
    WorkerCommand.TILE_COMPUTE: TileTask,
    WorkerCommand.ONLINE_STORE_TABLE_CLEANUP: OnlineStoreCleanupTask,
    WorkerCommand.QUERY_CACHE_CLEANUP: QueryCacheCleanupTask,
    WorkerCommand.SCHEDULED_FEATURE_MATERIALIZE: ScheduledFeatureMaterializeTask,
    WorkerCommand.FEATURE_MATERIALIZE_SYNC: FeatureMaterializeSyncTask,
    WorkerCommand.CATALOG_ONLINE_STORE_UPDATE: CatalogOnlineStoreUpdateTask,
    WorkerCommand.DATA_DESCRIPTION: DataDescriptionTask,
    WorkerCommand.TABLE_VALIDATION: TableValidationTask,
    WorkerCommand.TEST: TestTask,
    WorkerCommand.IO_TEST: TestIOTask,
    WorkerCommand.CATALOG_CLEANUP: CatalogCleanupTask,
    WorkerCommand.DEVELOPMENT_DATASET_CREATE: DevelopmentDatasetCreateTask,
    WorkerCommand.DEVELOPMENT_DATASET_DELETE: DevelopmentDatasetDeleteTask,
    WorkerCommand.DEVELOPMENT_DATASET_ADD_TABLES: DevelopmentDatasetAddTablesTask,
    # TO BE DEPRECATED
    WorkerCommand.FEATURE_LIST_CREATE_WITH_BATCH_FEATURE_CREATE: FeatureListCreateWithBatchFeatureCreationTask,
}
