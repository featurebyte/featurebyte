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
from featurebyte.routes.user_defined_function.controller import UserDefinedFunctionController
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
from featurebyte.service.user_defined_function import UserDefinedFunctionService
from featurebyte.service.user_service import UserService
from featurebyte.service.validator.production_ready_validator import ProductionReadyValidator
from featurebyte.service.version import VersionService
from featurebyte.service.view_construction import ViewConstructionService

app_container_config = AppContainerConfig()

app_container_config.add_service_with_extra_deps(
    "session_validator_service",
    SessionValidatorService,
    ["credential_provider", "feature_store_service"],
)
app_container_config.add_class_with_deps(
    "production_ready_validator",
    ProductionReadyValidator,
    ["table_service", "feature_service", "version_service"],
)
app_container_config.add_class_with_deps(
    "table_info_service",
    TableInfoService,
    ["entity_service", "semantic_service", "catalog_service"],
)
app_container_config.add_service_with_extra_deps(
    "session_manager_service",
    SessionManagerService,
    [
        "credential_provider",
        "session_validator_service",
    ],
)
app_container_config.add_service_with_extra_deps(
    "parent_entity_lookup_service",
    ParentEntityLookupService,
    [
        "entity_service",
        "table_service",
    ],
)
app_container_config.add_service_with_extra_deps(
    "online_enable_service",
    OnlineEnableService,
    [
        "session_manager_service",
        "task_manager",
        "feature_service",
        "feature_store_service",
        "feature_namespace_service",
        "feature_list_service",
        "online_store_table_version_service",
    ],
)
app_container_config.add_service_with_extra_deps(
    "entity_validation_service",
    EntityValidationService,
    ["entity_service", "parent_entity_lookup_service"],
)
app_container_config.add_service_with_extra_deps(
    "online_serving_service",
    OnlineServingService,
    [
        "session_manager_service",
        "entity_validation_service",
        "online_store_table_version_service",
        "feature_store_service",
    ],
)
app_container_config.add_service_with_extra_deps(
    "feature_list_status_service",
    FeatureListStatusService,
    ["feature_list_namespace_service", "feature_list_service"],
)
app_container_config.add_service_with_extra_deps(
    "deploy_service",
    DeployService,
    [
        "feature_service",
        "online_enable_service",
        "feature_list_status_service",
        "deployment_service",
        "feature_list_namespace_service",
        "feature_list_service",
    ],
)
app_container_config.add_service_with_extra_deps(
    "preview_service",
    PreviewService,
    [
        "session_manager_service",
        "feature_list_service",
        "entity_validation_service",
        "feature_store_service",
    ],
)
app_container_config.add_service_with_extra_deps(
    "feature_store_warehouse_service",
    FeatureStoreWarehouseService,
    [
        "session_manager_service",
        "feature_store_service",
    ],
)
app_container_config.add_service_with_extra_deps(
    "context_service", ContextService, ["entity_service"]
)
app_container_config.add_service_with_extra_deps(
    "entity_service", EntityService, ["catalog_service"]
)
app_container_config.add_basic_service("dimension_table_service", DimensionTableService)
app_container_config.add_basic_service("event_table_service", EventTableService)
app_container_config.add_basic_service("item_table_service", ItemTableService)
app_container_config.add_basic_service("scd_table_service", SCDTableService)
app_container_config.add_service_with_extra_deps(
    "feature_service", FeatureService, ["table_service", "view_construction_service"]
)
app_container_config.add_service_with_extra_deps(
    "feature_list_service",
    FeatureListService,
    [
        "entity_service",
        "relationship_info_service",
        "feature_service",
        "feature_list_namespace_service",
    ],
)
app_container_config.add_basic_service("deployment_service", DeploymentService)
app_container_config.add_service_with_extra_deps(
    "user_defined_function_service",
    UserDefinedFunctionService,
    ["feature_store_service"],
)
app_container_config.add_basic_service(
    "online_store_table_version_service", OnlineStoreTableVersionService
)
app_container_config.add_service_with_extra_deps(
    "observation_table_service",
    ObservationTableService,
    [
        "feature_store_service",
        "context_service",
    ],
)
app_container_config.add_service_with_extra_deps(
    "historical_feature_table_service",
    HistoricalFeatureTableService,
    ["feature_store_service"],
)
app_container_config.add_service_with_extra_deps(
    "batch_request_table_service",
    BatchRequestTableService,
    [
        "feature_store_service",
        "context_service",
    ],
)
app_container_config.add_service_with_extra_deps(
    "batch_feature_table_service",
    BatchFeatureTableService,
    ["feature_store_service"],
)
app_container_config.add_service_with_extra_deps(
    "static_source_table_service",
    StaticSourceTableService,
    [
        "feature_store_service",
    ],
)
app_container_config.add_service_with_extra_deps(
    "feature_readiness_service",
    FeatureReadinessService,
    [
        "feature_service",
        "feature_namespace_service",
        "feature_list_service",
        "feature_list_namespace_service",
        "production_ready_validator",
    ],
)
app_container_config.add_service_with_extra_deps(
    "table_status_service",
    TableStatusService,
    [
        "feature_service",
        "feature_readiness_service",
    ],
)
app_container_config.add_service_with_extra_deps(
    "feature_job_setting_analysis_service",
    FeatureJobSettingAnalysisService,
    ["event_table_service"],
)
app_container_config.add_service_with_extra_deps(
    "feature_list_namespace_service",
    FeatureListNamespaceService,
    ["entity_service", "table_service", "catalog_service", "feature_namespace_service"],
)
app_container_config.add_basic_service("feature_namespace_service", FeatureNamespaceService)
app_container_config.add_service_with_extra_deps(
    "table_columns_info_service",
    TableColumnsInfoService,
    [
        "semantic_service",
        "entity_service",
        "relationship_info_service",
        "entity_relationship_service",
    ],
)
app_container_config.add_service_with_extra_deps(
    "default_version_mode_service",
    DefaultVersionModeService,
    ["feature_namespace_service", "feature_readiness_service", "feature_list_namespace_service"],
)
app_container_config.add_basic_service("feature_store_service", FeatureStoreService)
app_container_config.add_basic_service("semantic_service", SemanticService)
app_container_config.add_basic_service("table_service", TableService)
app_container_config.add_service_with_extra_deps(
    "version_service",
    VersionService,
    [
        "table_service",
        "feature_service",
        "feature_namespace_service",
        "feature_list_service",
        "feature_list_namespace_service",
        "view_construction_service",
    ],
)
app_container_config.add_basic_service("entity_relationship_service", EntityRelationshipService)
app_container_config.add_basic_service("semantic_relationship_service", SemanticRelationshipService)
app_container_config.add_basic_service("catalog_service", CatalogService)
app_container_config.add_basic_service("target_service", TargetService)
app_container_config.add_basic_service("relationship_info_service", RelationshipInfoService)
app_container_config.add_basic_service("user_service", UserService)
app_container_config.add_service_with_extra_deps(
    "view_construction_service", ViewConstructionService, ["table_service"]
)
app_container_config.add_basic_service("periodic_task_service", PeriodicTaskService)
app_container_config.add_service_with_extra_deps(
    "credential_service",
    CredentialService,
    ["feature_store_warehouse_service", "feature_store_service"],
)

app_container_config.add_class_with_deps(
    "target_controller",
    TargetController,
    ["target_service", "entity_service"],
)
app_container_config.add_class_with_deps(
    "relationship_info_controller",
    RelationshipInfoController,
    ["relationship_info_service", "entity_service", "table_service", "user_service"],
)
app_container_config.add_class_with_deps(
    "context_controller", ContextController, ["context_service"]
)
app_container_config.add_class_with_deps(
    "entity_controller",
    EntityController,
    ["entity_service", "entity_relationship_service", "catalog_service"],
)
app_container_config.add_class_with_deps(
    "event_table_controller",
    EventTableController,
    [
        "event_table_service",
        "table_columns_info_service",
        "table_status_service",
        "semantic_service",
        "table_info_service",
    ],
)

app_container_config.add_class_with_deps(
    "dimension_table_controller",
    DimensionTableController,
    [
        "dimension_table_service",
        "table_columns_info_service",
        "table_status_service",
        "semantic_service",
        "table_info_service",
    ],
)
app_container_config.add_class_with_deps(
    "item_table_controller",
    ItemTableController,
    [
        "item_table_service",
        "table_columns_info_service",
        "table_status_service",
        "semantic_service",
        "table_info_service",
        "event_table_service",
    ],
)
app_container_config.add_class_with_deps(
    "scd_table_controller",
    SCDTableController,
    [
        "scd_table_service",
        "table_columns_info_service",
        "table_status_service",
        "semantic_service",
        "table_info_service",
    ],
)
app_container_config.add_class_with_deps(
    "feature_controller",
    FeatureController,
    [
        "feature_service",
        "feature_namespace_service",
        "entity_service",
        "feature_list_service",
        "feature_readiness_service",
        "preview_service",
        "version_service",
        "feature_store_warehouse_service",
        "task_controller",
        "catalog_service",
        "table_service",
        "feature_namespace_controller",
        "semantic_service",
    ],
)
app_container_config.add_class_with_deps(
    "feature_list_controller",
    FeatureListController,
    [
        "feature_list_service",
        "feature_list_namespace_service",
        "feature_service",
        "feature_readiness_service",
        "deploy_service",
        "preview_service",
        "version_service",
        "feature_store_warehouse_service",
        "task_controller",
    ],
)
app_container_config.add_class_with_deps(
    "feature_job_setting_analysis_controller",
    FeatureJobSettingAnalysisController,
    [
        "feature_job_setting_analysis_service",
        "task_controller",
        "event_table_service",
        "catalog_service",
    ],
)
app_container_config.add_class_with_deps(
    "feature_list_namespace_controller",
    FeatureListNamespaceController,
    [
        "feature_list_namespace_service",
        "entity_service",
        "feature_list_service",
        "default_version_mode_service",
        "feature_readiness_service",
        "feature_list_status_service",
    ],
)
app_container_config.add_class_with_deps(
    "feature_namespace_controller",
    FeatureNamespaceController,
    [
        "feature_namespace_service",
        "entity_service",
        "feature_service",
        "default_version_mode_service",
        "feature_readiness_service",
        "table_service",
        "catalog_service",
    ],
)
app_container_config.add_class_with_deps(
    "feature_store_controller",
    FeatureStoreController,
    [
        "feature_store_service",
        "preview_service",
        "session_manager_service",
        "session_validator_service",
        "feature_store_warehouse_service",
        "credential_service",
    ],
)

app_container_config.add_class_with_deps(
    "semantic_controller", SemanticController, ["semantic_service", "semantic_relationship_service"]
)
app_container_config.add_class_with_deps("table_controller", TableController, ["table_service"])
app_container_config.add_class_with_deps(
    "catalog_controller", CatalogController, ["catalog_service"]
)
app_container_config.add_class_with_deps(
    "periodic_task_controller", PeriodicTaskController, ["periodic_task_service"]
)
app_container_config.add_class_with_deps(
    "credential_controller", CredentialController, ["credential_service", "feature_store_service"]
)
app_container_config.add_class_with_deps(
    "observation_table_controller",
    ObservationTableController,
    [
        "observation_table_service",
        "preview_service",
        "historical_feature_table_service",
        "task_controller",
        "feature_store_service",
    ],
)
app_container_config.add_class_with_deps(
    "historical_feature_table_controller",
    HistoricalFeatureTableController,
    [
        "historical_feature_table_service",
        "preview_service",
        "feature_store_service",
        "observation_table_service",
        "entity_validation_service",
        "task_controller",
        "feature_list_service",
    ],
)
app_container_config.add_class_with_deps(
    "batch_request_table_controller",
    BatchRequestTableController,
    [
        "batch_request_table_service",
        "preview_service",
        "batch_feature_table_service",
        "task_controller",
        "feature_store_service",
    ],
)
app_container_config.add_class_with_deps(
    "batch_feature_table_controller",
    BatchFeatureTableController,
    [
        "batch_feature_table_service",
        "preview_service",
        "feature_store_service",
        "feature_list_service",
        "batch_request_table_service",
        "deployment_service",
        "entity_validation_service",
        "task_controller",
    ],
)
app_container_config.add_class_with_deps(
    "deployment_controller",
    DeploymentController,
    [
        "deployment_service",
        "catalog_service",
        "context_service",
        "feature_list_service",
        "online_serving_service",
        "task_controller",
    ],
)
app_container_config.add_class_with_deps(
    "static_source_table_controller",
    StaticSourceTableController,
    [
        "static_source_table_service",
        "preview_service",
        "table_service",
        "task_controller",
        "feature_store_service",
    ],
)
app_container_config.add_class_with_deps(
    "user_defined_function_controller",
    UserDefinedFunctionController,
    ["user_defined_function_service"],
)
