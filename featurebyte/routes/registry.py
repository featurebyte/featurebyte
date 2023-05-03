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
from featurebyte.routes.table.controller import TableController
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
from featurebyte.service.info import InfoService
from featurebyte.service.item_table import ItemTableService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.online_enable import OnlineEnableService
from featurebyte.service.online_serving import OnlineServingService
from featurebyte.service.parent_serving import ParentEntityLookupService
from featurebyte.service.periodic_task import PeriodicTaskService
from featurebyte.service.preview import PreviewService
from featurebyte.service.relationship import EntityRelationshipService, SemanticRelationshipService
from featurebyte.service.relationship_info import RelationshipInfoService
from featurebyte.service.scd_table import SCDTableService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.session_validator import SessionValidatorService
from featurebyte.service.table import TableService
from featurebyte.service.table_columns_info import TableColumnsInfoService
from featurebyte.service.table_status import TableStatusService
from featurebyte.service.user_service import UserService
from featurebyte.service.version import VersionService
from featurebyte.service.view_construction import ViewConstructionService

app_container_config = AppContainerConfig()

app_container_config.add_service_with_extra_deps(
    "session_validator_service", SessionValidatorService, ["credential_provider"]
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
    "online_enable_service", OnlineEnableService, ["session_manager_service"]
)
app_container_config.add_service_with_extra_deps(
    "entity_validation_service",
    EntityValidationService,
    ["entity_service", "parent_entity_lookup_service"],
)
app_container_config.add_service_with_extra_deps(
    "online_serving_service",
    OnlineServingService,
    ["session_manager_service", "entity_validation_service"],
)
app_container_config.add_service_with_extra_deps(
    "feature_list_status_service",
    FeatureListStatusService,
    ["feature_list_namespace_service", "feature_list_service"],
)
app_container_config.add_service_with_extra_deps(
    "deploy_service",
    DeployService,
    ["online_enable_service", "feature_list_status_service", "deployment_service"],
)
app_container_config.add_service_with_extra_deps(
    "preview_service",
    PreviewService,
    [
        "session_manager_service",
        "feature_list_service",
        "entity_validation_service",
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
app_container_config.add_basic_service("context_service", ContextService)
app_container_config.add_basic_service("entity_service", EntityService)
app_container_config.add_basic_service("dimension_table_service", DimensionTableService)
app_container_config.add_basic_service("event_table_service", EventTableService)
app_container_config.add_basic_service("item_table_service", ItemTableService)
app_container_config.add_basic_service("scd_table_service", SCDTableService)
app_container_config.add_basic_service("feature_service", FeatureService)
app_container_config.add_basic_service("feature_list_service", FeatureListService)
app_container_config.add_basic_service("deployment_service", DeploymentService)
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
    "feature_readiness_service",
    FeatureReadinessService,
    [
        "table_service",
        "feature_service",
        "feature_namespace_service",
        "feature_list_service",
        "feature_list_namespace_service",
        "version_service",
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
app_container_config.add_basic_service(
    "feature_job_setting_analysis_service", FeatureJobSettingAnalysisService
)
app_container_config.add_basic_service(
    "feature_list_namespace_service", FeatureListNamespaceService
)
app_container_config.add_basic_service("feature_namespace_service", FeatureNamespaceService)
app_container_config.add_basic_service("table_columns_info_service", TableColumnsInfoService)
app_container_config.add_service_with_extra_deps(
    "default_version_mode_service",
    DefaultVersionModeService,
    ["feature_namespace_service", "feature_readiness_service", "feature_list_namespace_service"],
)
app_container_config.add_basic_service("feature_store_service", FeatureStoreService)
app_container_config.add_basic_service("semantic_service", SemanticService)
app_container_config.add_basic_service("table_service", TableService)
app_container_config.add_basic_service("version_service", VersionService)
app_container_config.add_basic_service("entity_relationship_service", EntityRelationshipService)
app_container_config.add_basic_service("semantic_relationship_service", SemanticRelationshipService)
app_container_config.add_basic_service("info_service", InfoService)
app_container_config.add_basic_service("catalog_service", CatalogService)
app_container_config.add_basic_service("relationship_info_service", RelationshipInfoService)
app_container_config.add_basic_service("user_service", UserService)
app_container_config.add_basic_service("view_construction_service", ViewConstructionService)
app_container_config.add_basic_service("periodic_task_service", PeriodicTaskService)
app_container_config.add_service_with_extra_deps(
    "credential_service",
    CredentialService,
    ["feature_store_warehouse_service"],
)

app_container_config.add_controller(
    "relationship_info_controller",
    RelationshipInfoController,
    ["relationship_info_service", "info_service", "entity_service", "table_service"],
)
app_container_config.add_controller("context_controller", ContextController, ["context_service"])
app_container_config.add_controller(
    "entity_controller",
    EntityController,
    ["entity_service", "entity_relationship_service", "info_service"],
)
app_container_config.add_controller(
    "event_table_controller",
    EventTableController,
    [
        "event_table_service",
        "table_columns_info_service",
        "table_status_service",
        "semantic_service",
        "info_service",
    ],
)

app_container_config.add_controller(
    "dimension_table_controller",
    DimensionTableController,
    [
        "dimension_table_service",
        "table_columns_info_service",
        "table_status_service",
        "semantic_service",
        "info_service",
    ],
)
app_container_config.add_controller(
    "item_table_controller",
    ItemTableController,
    [
        "item_table_service",
        "table_columns_info_service",
        "table_status_service",
        "semantic_service",
        "info_service",
    ],
)
app_container_config.add_controller(
    "scd_table_controller",
    SCDTableController,
    [
        "scd_table_service",
        "table_columns_info_service",
        "table_status_service",
        "semantic_service",
        "info_service",
    ],
)
app_container_config.add_controller(
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
        "info_service",
        "feature_store_warehouse_service",
    ],
)
app_container_config.add_controller(
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
        "info_service",
        "feature_store_warehouse_service",
    ],
)
app_container_config.add_controller(
    "feature_job_setting_analysis_controller",
    FeatureJobSettingAnalysisController,
    ["feature_job_setting_analysis_service", "task_controller", "info_service"],
)
app_container_config.add_controller(
    "feature_list_namespace_controller",
    FeatureListNamespaceController,
    [
        "feature_list_namespace_service",
        "feature_list_service",
        "default_version_mode_service",
        "feature_readiness_service",
        "feature_list_status_service",
        "info_service",
    ],
)
app_container_config.add_controller(
    "feature_namespace_controller",
    FeatureNamespaceController,
    [
        "feature_namespace_service",
        "entity_service",
        "feature_service",
        "default_version_mode_service",
        "feature_readiness_service",
        "info_service",
    ],
)
app_container_config.add_controller(
    "feature_store_controller",
    FeatureStoreController,
    [
        "feature_store_service",
        "preview_service",
        "info_service",
        "session_manager_service",
        "session_validator_service",
        "feature_store_warehouse_service",
        "credential_service",
    ],
)

app_container_config.add_controller(
    "semantic_controller", SemanticController, ["semantic_service", "semantic_relationship_service"]
)
app_container_config.add_controller("table_controller", TableController, ["table_service"])
app_container_config.add_controller(
    "catalog_controller", CatalogController, ["catalog_service", "info_service"]
)
app_container_config.add_controller(
    "periodic_task_controller", PeriodicTaskController, ["periodic_task_service"]
)
app_container_config.add_controller(
    "credential_controller", CredentialController, ["credential_service", "info_service"]
)
app_container_config.add_controller(
    "observation_table_controller",
    ObservationTableController,
    [
        "observation_table_service",
        "preview_service",
        "historical_feature_table_service",
        "info_service",
        "task_controller",
    ],
)
app_container_config.add_controller(
    "historical_feature_table_controller",
    HistoricalFeatureTableController,
    [
        "historical_feature_table_service",
        "preview_service",
        "feature_store_service",
        "observation_table_service",
        "entity_validation_service",
        "info_service",
        "task_controller",
    ],
)
app_container_config.add_controller(
    "batch_request_table_controller",
    BatchRequestTableController,
    [
        "batch_request_table_service",
        "preview_service",
        "batch_feature_table_service",
        "info_service",
        "task_controller",
    ],
)
app_container_config.add_controller(
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
        "info_service",
        "task_controller",
    ],
)
app_container_config.add_controller(
    "deployment_controller",
    DeploymentController,
    [
        "deployment_service",
        "catalog_service",
        "context_service",
        "feature_list_service",
        "online_serving_service",
        "info_service",
        "task_controller",
    ],
)
