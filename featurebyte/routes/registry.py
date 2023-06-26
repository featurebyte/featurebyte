"""
Registrations module.

This contains all the dependencies that we want to register in order to get our fast API app up and running.
"""
from featurebyte.routes.app_container_config import AppContainerConfig
from featurebyte.routes.batch_feature_table.controller import BatchFeatureTableController
from featurebyte.routes.batch_request_table.controller import BatchRequestTableController
from featurebyte.routes.catalog.controller import CatalogController
from featurebyte.routes.context.controller import ContextController
from featurebyte.routes.credential.controller import CredentialController
from featurebyte.routes.deployment.controller import DeploymentController
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
from featurebyte.routes.task.controller import TaskController
from featurebyte.routes.temp_data.controller import TempDataController
from featurebyte.service.batch_feature_table import BatchFeatureTableService
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.catalog import CatalogService
from featurebyte.service.context import ContextService
from featurebyte.service.credential import CredentialService
from featurebyte.service.default_version_mode import DefaultVersionModeService
from featurebyte.service.deploy import DeployService
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.dimension_table import DimensionTableService
from featurebyte.service.entity import EntityService
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.event_table import EventTableService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_list_status import FeatureListStatusService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_readiness import FeatureReadinessService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.item_table import ItemTableService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.online_enable import OnlineEnableService
from featurebyte.service.online_serving import OnlineServingService
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
from featurebyte.service.table_info import TableInfoService
from featurebyte.service.table_status import TableStatusService
from featurebyte.service.target import TargetService
from featurebyte.service.task_manager import TaskManager
from featurebyte.service.user_service import UserService
from featurebyte.service.validator.production_ready_validator import ProductionReadyValidator
from featurebyte.service.version import VersionService
from featurebyte.service.view_construction import ViewConstructionService
from featurebyte.utils.credential import MongoBackedCredentialProvider

app_container_config = AppContainerConfig()

app_container_config.register_service(SessionValidatorService)
app_container_config.register_class(ProductionReadyValidator)
app_container_config.register_class(TableInfoService)
app_container_config.register_service(SessionManagerService)
app_container_config.register_service(ParentEntityLookupService)
app_container_config.register_service(OnlineEnableService)
app_container_config.register_service(EntityValidationService)
app_container_config.register_service(OnlineServingService)
app_container_config.register_service(FeatureListStatusService)
app_container_config.register_service(DeployService)
app_container_config.register_service(PreviewService)
app_container_config.register_service(FeatureStoreWarehouseService)
app_container_config.register_service(ContextService)
app_container_config.register_service(EntityService)
app_container_config.register_service(DimensionTableService)
app_container_config.register_service(EventTableService)
app_container_config.register_service(ItemTableService)
app_container_config.register_service(SCDTableService, name_override="scd_table_service")
app_container_config.register_service(FeatureService)
app_container_config.register_service(FeatureListService)
app_container_config.register_service(DeploymentService)
app_container_config.register_service(OnlineStoreTableVersionService)
app_container_config.register_service(ObservationTableService)
app_container_config.register_service(HistoricalFeatureTableService)
app_container_config.register_service(BatchRequestTableService)
app_container_config.register_service(BatchFeatureTableService)
app_container_config.register_service(StaticSourceTableService)
app_container_config.register_service(FeatureReadinessService)
app_container_config.register_service(TableStatusService)
app_container_config.register_service(FeatureJobSettingAnalysisService)
app_container_config.register_service(FeatureListNamespaceService)
app_container_config.register_service(FeatureNamespaceService)
app_container_config.register_service(TableColumnsInfoService)
app_container_config.register_service(DefaultVersionModeService)
app_container_config.register_service(FeatureStoreService)
app_container_config.register_service(SemanticService)
app_container_config.register_service(TableService)
app_container_config.register_service(VersionService)
app_container_config.register_service(EntityRelationshipService)
app_container_config.register_service(SemanticRelationshipService)
app_container_config.register_service(CatalogService)
app_container_config.register_service(TargetService)
app_container_config.register_service(RelationshipInfoService)
app_container_config.register_service(UserService)
app_container_config.register_service(ViewConstructionService)
app_container_config.register_service(PeriodicTaskService)
app_container_config.register_service(CredentialService)
app_container_config.register_class(
    TargetController, dependency_override={"service": "target_service"}
)
app_container_config.register_class(RelationshipInfoController)
app_container_config.register_class(
    ContextController, dependency_override={"service": "context_service"}
)
app_container_config.register_class(EntityController)
app_container_config.register_class(EventTableController)
app_container_config.register_class(DimensionTableController)
app_container_config.register_class(ItemTableController)
app_container_config.register_class(
    SCDTableController,
    dependency_override={"service": "scd_table_service"},
    name_override="scd_table_controller",
)
app_container_config.register_class(FeatureController)
app_container_config.register_class(FeatureListController)
app_container_config.register_class(FeatureJobSettingAnalysisController)
app_container_config.register_class(FeatureListNamespaceController)
app_container_config.register_class(FeatureNamespaceController)
app_container_config.register_class(FeatureStoreController)
app_container_config.register_class(SemanticController)
app_container_config.register_class(
    TableController, dependency_override={"service": "table_service"}
)
app_container_config.register_class(
    CatalogController, dependency_override={"service": "catalog_service"}
)
app_container_config.register_class(
    PeriodicTaskController, dependency_override={"service": "periodic_task_service"}
)
app_container_config.register_class(CredentialController)
app_container_config.register_class(ObservationTableController)
app_container_config.register_class(HistoricalFeatureTableController)
app_container_config.register_class(BatchRequestTableController)
app_container_config.register_class(BatchFeatureTableController)
app_container_config.register_class(DeploymentController)
app_container_config.register_class(StaticSourceTableController)

# These have dependency overrides set as [] as they are manually initialized.
app_container_config.register_class(TaskController, force_no_deps=True)
app_container_config.register_class(
    TempDataController, force_no_deps=True, name_override="tempdata_controller"
)
app_container_config.register_class(
    MongoBackedCredentialProvider, force_no_deps=True, name_override="credential_provider"
)
app_container_config.register_class(TaskManager, force_no_deps=True)

# Validate the config after all classes have been registered.
# This should be the last line in this module.
app_container_config.validate()
