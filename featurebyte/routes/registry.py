"""
Registrations module.

This contains all the dependencies that we want to register in order to get our fast API app up and running.
"""

from celery import Celery
from redis import Redis

from featurebyte.migration.migration_data_service import SchemaMetadataService
from featurebyte.migration.service.data_warehouse import (
    DataWarehouseMigrationServiceV6,
    DataWarehouseMigrationServiceV8,
    TileColumnTypeExtractor,
)
from featurebyte.migration.service.mixin import DataWarehouseMigrationMixin
from featurebyte.models.base import User
from featurebyte.routes.app_container_config import AppContainerConfig
from featurebyte.routes.batch_feature_table.controller import BatchFeatureTableController
from featurebyte.routes.batch_request_table.controller import BatchRequestTableController
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.routes.catalog.catalog_name_injector import CatalogNameInjector
from featurebyte.routes.catalog.controller import CatalogController
from featurebyte.routes.common.derive_primary_entity_helper import DerivePrimaryEntityHelper
from featurebyte.routes.common.feature_metadata_extractor import FeatureOrTargetMetadataExtractor
from featurebyte.routes.common.feature_or_target_helper import FeatureOrTargetHelper
from featurebyte.routes.common.primary_entity_validator import PrimaryEntityValidator
from featurebyte.routes.context.controller import ContextController
from featurebyte.routes.credential.controller import CredentialController
from featurebyte.routes.deployment.controller import AllDeploymentController, DeploymentController
from featurebyte.routes.dimension_table.controller import DimensionTableController
from featurebyte.routes.entity.controller import EntityController
from featurebyte.routes.event_table.controller import EventTableController
from featurebyte.routes.feature.controller import FeatureController
from featurebyte.routes.feature_job_setting_analysis.controller import (
    FeatureJobSettingAnalysisController,
)
from featurebyte.routes.feature_list.controller import FeatureListController
from featurebyte.routes.feature_list_namespace.controller import FeatureListNamespaceController
from featurebyte.routes.feature_namespace.controller import FeatureNamespaceController
from featurebyte.routes.feature_store.controller import FeatureStoreController
from featurebyte.routes.historical_feature_table.controller import HistoricalFeatureTableController
from featurebyte.routes.item_table.controller import ItemTableController
from featurebyte.routes.observation_table.controller import ObservationTableController
from featurebyte.routes.periodic_tasks.controller import PeriodicTaskController
from featurebyte.routes.relationship_info.controller import RelationshipInfoController
from featurebyte.routes.scd_table.controller import SCDTableController
from featurebyte.routes.semantic.controller import SemanticController
from featurebyte.routes.static_source_table.controller import StaticSourceTableController
from featurebyte.routes.table.controller import TableController
from featurebyte.routes.target.controller import TargetController
from featurebyte.routes.target_namespace.controller import TargetNamespaceController
from featurebyte.routes.target_table.controller import TargetTableController
from featurebyte.routes.task.controller import TaskController
from featurebyte.routes.temp_data.controller import TempDataController
from featurebyte.routes.use_case.controller import UseCaseController
from featurebyte.routes.user_defined_function.controller import UserDefinedFunctionController
from featurebyte.service.batch_feature_table import BatchFeatureTableService
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.catalog import AllCatalogService, CatalogService
from featurebyte.service.context import ContextService
from featurebyte.service.credential import CredentialService
from featurebyte.service.deploy import DeployService
from featurebyte.service.deployment import AllDeploymentService, DeploymentService
from featurebyte.service.dimension_table import DimensionTableService
from featurebyte.service.entity import EntityService
from featurebyte.service.entity_relationship_extractor import EntityRelationshipExtractorService
from featurebyte.service.entity_serving_names import EntityServingNamesService
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.event_table import EventTableService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_facade import FeatureFacadeService
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService
from featurebyte.service.feature_list import AllFeatureListService, FeatureListService
from featurebyte.service.feature_list_facade import FeatureListFacadeService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_list_status import FeatureListStatusService
from featurebyte.service.feature_manager import FeatureManagerService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_preview import FeaturePreviewService
from featurebyte.service.feature_readiness import FeatureReadinessService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.historical_features import (
    HistoricalFeatureExecutor,
    HistoricalFeaturesService,
)
from featurebyte.service.item_table import ItemTableService
from featurebyte.service.namespace_handler import NamespaceHandler
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.online_enable import OnlineEnableService
from featurebyte.service.online_serving import OnlineServingService
from featurebyte.service.online_store_cleanup import OnlineStoreCleanupService
from featurebyte.service.online_store_cleanup_scheduler import OnlineStoreCleanupSchedulerService
from featurebyte.service.online_store_compute_query_service import OnlineStoreComputeQueryService
from featurebyte.service.online_store_table_version import OnlineStoreTableVersionService
from featurebyte.service.parent_serving import ParentEntityLookupService
from featurebyte.service.periodic_task import PeriodicTaskService
from featurebyte.service.preview import PreviewService
from featurebyte.service.relationship import EntityRelationshipService, SemanticRelationshipService
from featurebyte.service.relationship_info import RelationshipInfoService
from featurebyte.service.scd_table import SCDTableService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.session_validator import SessionValidatorService
from featurebyte.service.static_source_table import StaticSourceTableService
from featurebyte.service.table import TableService
from featurebyte.service.table_columns_info import TableColumnsInfoService
from featurebyte.service.table_facade import TableFacadeService
from featurebyte.service.table_info import TableInfoService
from featurebyte.service.table_status import TableStatusService
from featurebyte.service.target import TargetService
from featurebyte.service.target_helper.compute_target import TargetComputer, TargetExecutor
from featurebyte.service.target_namespace import TargetNamespaceService
from featurebyte.service.target_table import TargetTableService
from featurebyte.service.task_manager import TaskManager
from featurebyte.service.tile.tile_task_executor import TileTaskExecutor
from featurebyte.service.tile_cache import TileCacheService
from featurebyte.service.tile_job_log import TileJobLogService
from featurebyte.service.tile_manager import TileManagerService
from featurebyte.service.tile_registry_service import TileRegistryService
from featurebyte.service.tile_scheduler import TileSchedulerService
from featurebyte.service.use_case import UseCaseService
from featurebyte.service.user_defined_function import UserDefinedFunctionService
from featurebyte.service.user_service import UserService
from featurebyte.service.validator.entity_relationship_validator import (
    FeatureListEntityRelationshipValidator,
)
from featurebyte.service.validator.materialized_table_delete import ObservationTableDeleteValidator
from featurebyte.service.validator.production_ready_validator import ProductionReadyValidator
from featurebyte.service.version import VersionService
from featurebyte.service.view_construction import ViewConstructionService
from featurebyte.service.working_schema import WorkingSchemaService
from featurebyte.storage import Storage
from featurebyte.utils.credential import MongoBackedCredentialProvider
from featurebyte.utils.messaging import Progress
from featurebyte.utils.persistent import MongoDBImpl
from featurebyte.utils.storage import get_storage
from featurebyte.worker import get_redis
from featurebyte.worker.task.batch_feature_create import BatchFeatureCreateTask
from featurebyte.worker.task.batch_feature_table import BatchFeatureTableTask
from featurebyte.worker.task.batch_request_table import BatchRequestTableTask
from featurebyte.worker.task.deployment_create_update import DeploymentCreateUpdateTask
from featurebyte.worker.task.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisBacktestTask,
    FeatureJobSettingAnalysisTask,
)
from featurebyte.worker.task.feature_list_batch_feature_create import (
    FeatureListCreateWithBatchFeatureCreationTask,
)
from featurebyte.worker.task.feature_list_make_production_ready import (
    FeatureListMakeProductionReadyTask,
)
from featurebyte.worker.task.historical_feature_table import HistoricalFeatureTableTask
from featurebyte.worker.task.materialized_table_delete import MaterializedTableDeleteTask
from featurebyte.worker.task.observation_table import ObservationTableTask
from featurebyte.worker.task.observation_table_upload import ObservationTableUploadTask
from featurebyte.worker.task.online_store_cleanup import OnlineStoreCleanupTask
from featurebyte.worker.task.static_source_table import StaticSourceTableTask
from featurebyte.worker.task.target_table import TargetTableTask
from featurebyte.worker.task.test_task import TestTask
from featurebyte.worker.task.tile_task import TileTask
from featurebyte.worker.test_util.random_task import LongRunningTask, RandomTask
from featurebyte.worker.util.batch_feature_creator import BatchFeatureCreator
from featurebyte.worker.util.observation_set_helper import ObservationSetHelper
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater

app_container_config = AppContainerConfig()

# Register classes - please keep sorted by alphabetical order.
app_container_config.register_class(AllCatalogService)
app_container_config.register_class(AllDeploymentController)
app_container_config.register_class(AllDeploymentService)
app_container_config.register_class(AllFeatureListService)
app_container_config.register_class(BatchFeatureTableController)
app_container_config.register_class(BatchFeatureTableService)
app_container_config.register_class(BatchRequestTableController)
app_container_config.register_class(BatchRequestTableService)
app_container_config.register_class(
    CatalogController, dependency_override={"service": "catalog_service"}
)
app_container_config.register_class(CatalogNameInjector)
app_container_config.register_class(CatalogService)
app_container_config.register_class(
    ContextController, dependency_override={"service": "context_service"}
)
app_container_config.register_class(CredentialController)
app_container_config.register_class(CredentialService)
app_container_config.register_class(ContextService)
app_container_config.register_class(DataWarehouseMigrationMixin)
app_container_config.register_class(DataWarehouseMigrationServiceV6)
app_container_config.register_class(DataWarehouseMigrationServiceV8)
app_container_config.register_class(DeployService)
app_container_config.register_class(DeploymentController)
app_container_config.register_class(DeploymentService)
app_container_config.register_class(DerivePrimaryEntityHelper)
app_container_config.register_class(DimensionTableController)
app_container_config.register_class(DimensionTableService)
app_container_config.register_class(EntityController)
app_container_config.register_class(EntityRelationshipService)
app_container_config.register_class(EntityService)
app_container_config.register_class(EntityServingNamesService)
app_container_config.register_class(EntityValidationService)
app_container_config.register_class(EntityRelationshipExtractorService)
app_container_config.register_class(EventTableController)
app_container_config.register_class(EventTableService)
app_container_config.register_class(FeatureController)
app_container_config.register_class(FeatureService)
app_container_config.register_class(FeatureFacadeService)
app_container_config.register_class(FeatureJobSettingAnalysisController)
app_container_config.register_class(FeatureJobSettingAnalysisService)
app_container_config.register_class(FeatureListController)
app_container_config.register_class(FeatureListEntityRelationshipValidator)
app_container_config.register_class(FeatureListFacadeService)
app_container_config.register_class(FeatureListService)
app_container_config.register_class(FeatureListNamespaceController)
app_container_config.register_class(FeatureListNamespaceService)
app_container_config.register_class(FeatureListStatusService)
app_container_config.register_class(FeatureManagerService)
app_container_config.register_class(FeatureOrTargetHelper)
app_container_config.register_class(FeatureOrTargetMetadataExtractor)
app_container_config.register_class(FeatureNamespaceController)
app_container_config.register_class(FeatureNamespaceService)
app_container_config.register_class(FeaturePreviewService)
app_container_config.register_class(FeatureReadinessService)
app_container_config.register_class(FeatureStoreController)
app_container_config.register_class(FeatureStoreService)
app_container_config.register_class(FeatureStoreWarehouseService)
app_container_config.register_class(HistoricalFeatureExecutor)
app_container_config.register_class(HistoricalFeatureTableController)
app_container_config.register_class(HistoricalFeatureTableService)
app_container_config.register_class(
    HistoricalFeaturesService, dependency_override={"query_executor": "historical_feature_executor"}
)
app_container_config.register_class(ItemTableController)
app_container_config.register_class(ItemTableService)
app_container_config.register_class(MongoBackedCredentialProvider)
app_container_config.register_class(NamespaceHandler)
app_container_config.register_class(ObservationSetHelper)
app_container_config.register_class(ObservationTableController)
app_container_config.register_class(ObservationTableDeleteValidator)
app_container_config.register_class(ObservationTableService)
app_container_config.register_class(OnlineEnableService)
app_container_config.register_class(OnlineServingService)
app_container_config.register_class(OnlineStoreCleanupService)
app_container_config.register_class(OnlineStoreCleanupSchedulerService)
app_container_config.register_class(OnlineStoreComputeQueryService)
app_container_config.register_class(OnlineStoreTableVersionService)
app_container_config.register_class(ParentEntityLookupService)
app_container_config.register_class(
    PeriodicTaskController, dependency_override={"service": "periodic_task_service"}
)
app_container_config.register_class(PeriodicTaskService)
app_container_config.register_class(PreviewService)
app_container_config.register_class(ProductionReadyValidator)
app_container_config.register_class(RelationshipInfoController)
app_container_config.register_class(RelationshipInfoService)
app_container_config.register_class(
    SCDTableController,
    dependency_override={"service": "scd_table_service"},
)
app_container_config.register_class(SCDTableService)
app_container_config.register_class(SchemaMetadataService)
app_container_config.register_class(SemanticController)
app_container_config.register_class(SemanticService)
app_container_config.register_class(SemanticRelationshipService)
app_container_config.register_class(SessionManagerService)
app_container_config.register_class(SessionValidatorService)
app_container_config.register_class(StaticSourceTableController)
app_container_config.register_class(StaticSourceTableService)
app_container_config.register_class(TableColumnsInfoService)
app_container_config.register_class(
    TableController, dependency_override={"service": "table_service"}
)
app_container_config.register_class(TableFacadeService)
app_container_config.register_class(TableInfoService)
app_container_config.register_class(TableService)
app_container_config.register_class(TableStatusService)
app_container_config.register_class(
    TargetComputer, dependency_override={"query_executor": "target_executor"}
)
app_container_config.register_class(
    TargetController, dependency_override={"service": "target_service"}
)
app_container_config.register_class(TargetExecutor)
app_container_config.register_class(TargetService)
app_container_config.register_class(
    TargetNamespaceController, dependency_override={"service": "target_namespace_service"}
)
app_container_config.register_class(TargetNamespaceService)
app_container_config.register_class(TargetTableController)
app_container_config.register_class(TargetTableService)
app_container_config.register_class(TaskController)
app_container_config.register_class(TaskManager)
app_container_config.register_class(TempDataController)
app_container_config.register_class(TileCacheService)
app_container_config.register_class(TileColumnTypeExtractor)
app_container_config.register_class(TileJobLogService)
app_container_config.register_class(TileManagerService)
app_container_config.register_class(TileRegistryService)
app_container_config.register_class(TileSchedulerService)
app_container_config.register_class(TileTaskExecutor)
app_container_config.register_class(
    UserDefinedFunctionController, dependency_override={"service": "user_defined_function_service"}
)
app_container_config.register_class(UserDefinedFunctionService)
app_container_config.register_class(UserService)
app_container_config.register_class(VersionService)
app_container_config.register_class(ViewConstructionService)
app_container_config.register_class(WorkingSchemaService)
app_container_config.register_class(UseCaseService)
app_container_config.register_class(UseCaseController)
app_container_config.register_class(PrimaryEntityValidator)

# register tasks
app_container_config.register_class(TargetTableTask)
app_container_config.register_class(RandomTask)
app_container_config.register_class(FeatureJobSettingAnalysisTask)
app_container_config.register_class(FeatureJobSettingAnalysisBacktestTask)
app_container_config.register_class(HistoricalFeatureTableTask)
app_container_config.register_class(ObservationTableTask)
app_container_config.register_class(ObservationTableUploadTask)
app_container_config.register_class(DeploymentCreateUpdateTask)
app_container_config.register_class(BatchRequestTableTask)
app_container_config.register_class(BatchFeatureTableTask)
app_container_config.register_class(MaterializedTableDeleteTask)
app_container_config.register_class(BatchFeatureCreateTask)
app_container_config.register_class(FeatureListCreateWithBatchFeatureCreationTask)
app_container_config.register_class(StaticSourceTableTask)
app_container_config.register_class(TileTask)
app_container_config.register_class(OnlineStoreCleanupTask)
app_container_config.register_class(LongRunningTask)
app_container_config.register_class(TestTask)
app_container_config.register_class(FeatureListMakeProductionReadyTask)
app_container_config.register_class(TaskProgressUpdater)
app_container_config.register_class(BatchFeatureCreator)
app_container_config.register_class(BlockModificationHandler)
app_container_config.register_class(MongoDBImpl, name_override="persistent")

app_container_config.register_factory_method(get_storage)
app_container_config.register_factory_method(get_redis, name_override="redis")

# These have force_no_deps set as True, as they are manually initialized.
app_container_config.register_class(Celery, force_no_deps=True)
app_container_config.register_class(Storage, force_no_deps=True, name_override="temp_storage")
app_container_config.register_class(User, force_no_deps=True)


class Placeholder:
    """
    This is a special placeholder class that is used to inject the container config.
    """


# This looks a little funny right now, but every entry in the instance map must currently be found in the
# app_container_config, as some validation checks depend on it. For this particular case, we inject the
# catalog_id directly into the `instance_map` in the LazyAppContainer constructor. As such, we need an item in the
# app_container_config that is called `catalog_id`. This class of CatalogId will get parsed into `catalog_id`, and
# as such, works as a placeholder.
app_container_config.register_class(Placeholder, force_no_deps=True, name_override="catalog_id")


# Manually initialized via tasks.
app_container_config.register_class(Placeholder, force_no_deps=True, name_override="redis_uri")
app_container_config.register_class(Placeholder, force_no_deps=True, name_override="user_id")
app_container_config.register_class(Placeholder, force_no_deps=True, name_override="task_id")
app_container_config.register_class(Progress)

# Validate the config after all classes have been registered.
# This should be the last line in this module.
app_container_config.validate()
