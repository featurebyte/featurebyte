"""
Registrations module.

This contains all the dependencies that we want to register in order to get our fast API app up and running.
"""
from featurebyte.routes.app_container_config import AppContainerConfig
from featurebyte.routes.context.controller import ContextController
from featurebyte.routes.dimension_data.controller import DimensionDataController
from featurebyte.routes.entity.controller import EntityController
from featurebyte.routes.event_data.controller import EventDataController
from featurebyte.routes.feature.controller import FeatureController
from featurebyte.routes.feature_job_setting_analysis.controller import (
    FeatureJobSettingAnalysisController,
)
from featurebyte.routes.feature_list.controller import FeatureListController
from featurebyte.routes.feature_list_namespace.controller import FeatureListNamespaceController
from featurebyte.routes.feature_namespace.controller import FeatureNamespaceController
from featurebyte.routes.feature_store.controller import FeatureStoreController
from featurebyte.routes.item_data.controller import ItemDataController
from featurebyte.routes.periodic_tasks.controller import PeriodicTaskController
from featurebyte.routes.relationship_info.controller import RelationshipInfoController
from featurebyte.routes.scd_data.controller import SCDDataController
from featurebyte.routes.semantic.controller import SemanticController
from featurebyte.routes.tabular_data.controller import TabularDataController
from featurebyte.routes.workspace.controller import WorkspaceController
from featurebyte.service.context import ContextService
from featurebyte.service.data_update import DataUpdateService
from featurebyte.service.default_version_mode import DefaultVersionModeService
from featurebyte.service.deploy import DeployService
from featurebyte.service.dimension_data import DimensionDataService
from featurebyte.service.entity import EntityService
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.event_data import EventDataService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_readiness import FeatureReadinessService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.info import InfoService
from featurebyte.service.item_data import ItemDataService
from featurebyte.service.online_enable import OnlineEnableService
from featurebyte.service.online_serving import OnlineServingService
from featurebyte.service.parent_serving import ParentEntityLookupService
from featurebyte.service.periodic_task import PeriodicTaskService
from featurebyte.service.preview import PreviewService
from featurebyte.service.relationship import EntityRelationshipService, SemanticRelationshipService
from featurebyte.service.relationship_info import RelationshipInfoService
from featurebyte.service.scd_data import SCDDataService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.session_validator import SessionValidatorService
from featurebyte.service.tabular_data import DataService
from featurebyte.service.user_service import UserService
from featurebyte.service.version import VersionService
from featurebyte.service.view_construction import ViewConstructionService
from featurebyte.service.workspace import WorkspaceService
from featurebyte.utils.credential import ConfigCredentialProvider

app_container_config = AppContainerConfig()
app_container_config.add_no_dep_objects("credential_provider", ConfigCredentialProvider)

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
        "tabular_data_service",
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
    "deploy_service", DeployService, ["online_enable_service"]
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
app_container_config.add_basic_service("dimension_data_service", DimensionDataService)
app_container_config.add_basic_service("event_data_service", EventDataService)
app_container_config.add_basic_service("item_data_service", ItemDataService)
app_container_config.add_basic_service("scd_data_service", SCDDataService)
app_container_config.add_basic_service("feature_service", FeatureService)
app_container_config.add_basic_service("feature_list_service", FeatureListService)
app_container_config.add_service_with_extra_deps(
    "feature_readiness_service",
    FeatureReadinessService,
    [
        "feature_service",
        "feature_namespace_service",
        "feature_list_service",
        "feature_list_namespace_service",
    ],
)
app_container_config.add_basic_service(
    "feature_job_setting_analysis_service", FeatureJobSettingAnalysisService
)
app_container_config.add_basic_service(
    "feature_list_namespace_service", FeatureListNamespaceService
)
app_container_config.add_basic_service("feature_namespace_service", FeatureNamespaceService)
app_container_config.add_basic_service("data_update_service", DataUpdateService)
app_container_config.add_service_with_extra_deps(
    "default_version_mode_service",
    DefaultVersionModeService,
    ["feature_namespace_service", "feature_readiness_service", "feature_list_namespace_service"],
)
app_container_config.add_basic_service("feature_store_service", FeatureStoreService)
app_container_config.add_basic_service("semantic_service", SemanticService)
app_container_config.add_basic_service("tabular_data_service", DataService)
app_container_config.add_basic_service("version_service", VersionService)
app_container_config.add_basic_service("entity_relationship_service", EntityRelationshipService)
app_container_config.add_basic_service("semantic_relationship_service", SemanticRelationshipService)
app_container_config.add_basic_service("info_service", InfoService)
app_container_config.add_basic_service("workspace_service", WorkspaceService)
app_container_config.add_basic_service("relationship_info_service", RelationshipInfoService)
app_container_config.add_basic_service("user_service", UserService)
app_container_config.add_basic_service("view_construction_service", ViewConstructionService)
app_container_config.add_basic_service("periodic_task_service", PeriodicTaskService)

app_container_config.add_controller(
    "relationship_info_controller",
    RelationshipInfoController,
    ["relationship_info_service", "info_service", "entity_service", "tabular_data_service"],
)
app_container_config.add_controller("context_controller", ContextController, ["context_service"])
app_container_config.add_controller(
    "entity_controller",
    EntityController,
    ["entity_service", "entity_relationship_service", "info_service"],
)
app_container_config.add_controller(
    "event_data_controller",
    EventDataController,
    [
        "event_data_service",
        "data_update_service",
        "semantic_service",
        "info_service",
    ],
)

app_container_config.add_controller(
    "dimension_data_controller",
    DimensionDataController,
    [
        "dimension_data_service",
        "data_update_service",
        "semantic_service",
        "info_service",
    ],
)
app_container_config.add_controller(
    "item_data_controller",
    ItemDataController,
    [
        "item_data_service",
        "data_update_service",
        "semantic_service",
        "info_service",
    ],
)
app_container_config.add_controller(
    "scd_data_controller",
    SCDDataController,
    [
        "scd_data_service",
        "data_update_service",
        "semantic_service",
        "info_service",
    ],
)
app_container_config.add_controller(
    "feature_controller",
    FeatureController,
    [
        "feature_service",
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
        "feature_readiness_service",
        "deploy_service",
        "preview_service",
        "version_service",
        "info_service",
        "online_serving_service",
        "feature_store_warehouse_service",
        "feature_service",
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
        "default_version_mode_service",
        "feature_readiness_service",
        "info_service",
    ],
)
app_container_config.add_controller(
    "feature_namespace_controller",
    FeatureNamespaceController,
    [
        "feature_namespace_service",
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
    ],
)

app_container_config.add_controller(
    "semantic_controller", SemanticController, ["semantic_service", "semantic_relationship_service"]
)
app_container_config.add_controller(
    "tabular_data_controller",
    TabularDataController,
    [
        "tabular_data_service",
    ],
)
app_container_config.add_controller(
    "workspace_controller", WorkspaceController, ["workspace_service", "info_service"]
)
app_container_config.add_controller(
    "periodic_task_controller", PeriodicTaskController, ["periodic_task_service"]
)
