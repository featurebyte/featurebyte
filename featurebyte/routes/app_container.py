"""
Container for Controller objects to enable Dependency Injection
"""
from typing import Any, Dict

from featurebyte.persistent import Persistent
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
from featurebyte.routes.scd_data.controller import SCDDataController
from featurebyte.routes.semantic.controller import SemanticController
from featurebyte.routes.tabular_data.controller import TabularDataController
from featurebyte.routes.task.controller import TaskController
from featurebyte.routes.temp_data.controller import TempDataController
from featurebyte.service.context import ContextService
from featurebyte.service.data_update import DataUpdateService
from featurebyte.service.default_version_mode import DefaultVersionModeService
from featurebyte.service.deploy import DeployService
from featurebyte.service.dimension_data import DimensionDataService
from featurebyte.service.entity import EntityService
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
from featurebyte.service.preview import PreviewService
from featurebyte.service.relationship import EntityRelationshipService, SemanticRelationshipService
from featurebyte.service.scd_data import SCDDataService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.session_validator import SessionValidatorService
from featurebyte.service.tabular_data import DataService
from featurebyte.service.task_manager import AbstractTaskManager
from featurebyte.service.version import VersionService
from featurebyte.storage import Storage
from featurebyte.utils.credential import ConfigCredentialProvider

app_container_config = {
    # These are objects which don't take in any dependencies, and can be instantiated as is.
    "no_dependency_objects": [
        {
            "name": "credential_provider",
            "clazz": ConfigCredentialProvider,
        }
    ],
    # These services have dependencies in addition to the normal user, and persistent dependencies.
    "services_with_extra_deps": [
        {
            "name": "session_validator_service",
            "clazz": SessionValidatorService,
            "extra_deps": [
                "credential_provider",
            ],
        },
        {
            "name": "session_manager_service",
            "clazz": SessionManagerService,
            "extra_deps": [
                "credential_provider",
                "session_validator_service",
            ],
        },
        {
            "name": "online_enable_service",
            "clazz": OnlineEnableService,
            "extra_deps": [
                "session_manager_service",
            ],
        },
        {
            "name": "online_serving_service",
            "clazz": OnlineServingService,
            "extra_deps": [
                "session_manager_service",
            ],
        },
        {
            "name": "deploy_service",
            "clazz": DeployService,
            "extra_deps": [
                "online_enable_service",
            ],
        },
        {
            "name": "preview_service",
            "clazz": PreviewService,
            "extra_deps": [
                "session_manager_service",
            ],
        },
        {
            "name": "feature_store_warehouse_service",
            "clazz": FeatureStoreWarehouseService,
            "extra_deps": [
                "session_manager_service",
                "feature_store_service",
            ],
        },
    ],
    # These services only require the user, and persistent dependencies.
    "services": [
        {
            "name": "context_service",
            "clazz": ContextService,
        },
        {
            "name": "entity_service",
            "clazz": EntityService,
        },
        {
            "name": "dimension_data_service",
            "clazz": DimensionDataService,
        },
        {
            "name": "event_data_service",
            "clazz": EventDataService,
        },
        {
            "name": "item_data_service",
            "clazz": ItemDataService,
        },
        {
            "name": "scd_data_service",
            "clazz": SCDDataService,
        },
        {
            "name": "feature_service",
            "clazz": FeatureService,
        },
        {
            "name": "feature_list_service",
            "clazz": FeatureListService,
        },
        {
            "name": "feature_readiness_service",
            "clazz": FeatureReadinessService,
        },
        {
            "name": "feature_job_setting_analysis_service",
            "clazz": FeatureJobSettingAnalysisService,
        },
        {
            "name": "feature_list_namespace_service",
            "clazz": FeatureListNamespaceService,
        },
        {
            "name": "feature_namespace_service",
            "clazz": FeatureNamespaceService,
        },
        {
            "name": "data_update_service",
            "clazz": DataUpdateService,
        },
        {
            "name": "default_version_mode_service",
            "clazz": DefaultVersionModeService,
        },
        {
            "name": "feature_store_service",
            "clazz": FeatureStoreService,
        },
        {
            "name": "semantic_service",
            "clazz": SemanticService,
        },
        {
            "name": "tabular_data_service",
            "clazz": DataService,
        },
        {
            "name": "version_service",
            "clazz": VersionService,
        },
        {
            "name": "entity_relationship_service",
            "clazz": EntityRelationshipService,
        },
        {
            "name": "semantic_relationship_service",
            "clazz": SemanticRelationshipService,
        },
        {
            "name": "info_service",
            "clazz": InfoService,
        },
    ],
    # Controllers can depend on any object defined above.
    "controllers": [
        {
            "name": "context_controller",
            "clazz": ContextController,
            "depends": ["context_service"],
        },
        {
            "name": "entity_controller",
            "clazz": EntityController,
            "depends": ["entity_service", "entity_relationship_service", "info_service"],
        },
        {
            "name": "event_data_controller",
            "clazz": EventDataController,
            "depends": [
                "event_data_service",
                "data_update_service",
                "semantic_service",
                "info_service",
            ],
        },
        {
            "name": "dimension_data_controller",
            "clazz": DimensionDataController,
            "depends": [
                "dimension_data_service",
                "data_update_service",
                "semantic_service",
                "info_service",
            ],
        },
        {
            "name": "item_data_controller",
            "clazz": ItemDataController,
            "depends": [
                "item_data_service",
                "data_update_service",
                "semantic_service",
                "info_service",
            ],
        },
        {
            "name": "scd_data_controller",
            "clazz": SCDDataController,
            "depends": [
                "scd_data_service",
                "data_update_service",
                "semantic_service",
                "info_service",
            ],
        },
        {
            "name": "feature_controller",
            "clazz": FeatureController,
            "depends": [
                "feature_service",
                "feature_list_service",
                "feature_readiness_service",
                "preview_service",
                "version_service",
                "info_service",
                "feature_store_warehouse_service",
            ],
        },
        {
            "name": "feature_list_controller",
            "clazz": FeatureListController,
            "depends": [
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
        },
        {
            "name": "feature_job_setting_analysis_controller",
            "clazz": FeatureJobSettingAnalysisController,
            "depends": ["feature_job_setting_analysis_service", "task_controller", "info_service"],
        },
        {
            "name": "feature_list_namespace_controller",
            "clazz": FeatureListNamespaceController,
            "depends": [
                "feature_list_namespace_service",
                "default_version_mode_service",
                "info_service",
            ],
        },
        {
            "name": "feature_namespace_controller",
            "clazz": FeatureNamespaceController,
            "depends": [
                "feature_namespace_service",
                "default_version_mode_service",
                "info_service",
            ],
        },
        {
            "name": "feature_store_controller",
            "clazz": FeatureStoreController,
            "depends": [
                "feature_store_service",
                "preview_service",
                "info_service",
                "session_manager_service",
                "session_validator_service",
                "feature_store_warehouse_service",
            ],
        },
        {
            "name": "semantic_controller",
            "clazz": SemanticController,
            "depends": ["semantic_service", "semantic_relationship_service"],
        },
        {
            "name": "tabular_data_controller",
            "clazz": TabularDataController,
            "depends": [
                "tabular_data_service",
            ],
        },
    ],
}


class AppContainer:
    """
    App Container
    """

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        temp_storage: Storage,
        task_manager: AbstractTaskManager,
        storage: Storage,
        app_config: Dict[str, Any],
    ):
        """
        Initialize services and controller instances

        Parameters
        ----------
        user: User
            normal user
        persistent: Persistent
            persistent instance
        temp_storage: Storage
            temp storage
        task_manager: AbstractTaskManager
            task manager
        storage: Storage
            permanent storage
        app_config: Dict[str, Any]
            input app config dict, default to app_container_config
        """
        _ = storage  # not used in the app_container bean yet

        self.instance_map: Dict[str, Any] = {
            "task_controller": TaskController(task_manager=task_manager),
            "tempdata_controller": TempDataController(temp_storage=temp_storage),
        }

        # validate that there are no clashing names
        seen_names = set()
        for definitions in app_config.values():
            for definition in definitions:
                definition_name = definition["name"]
                if definition_name in seen_names:
                    raise ValueError(
                        f"error creating dependency map. {definition_name} has been defined already. "
                        "Consider changing the name of the dependency."
                    )
                seen_names.add(definition_name)

        # build no dependency objects
        for item in app_config["no_dependency_objects"]:
            name, clazz = item["name"], item["clazz"]
            self.instance_map[name] = clazz()

        # build services
        for item in app_config["services"]:
            name, clazz = item["name"], item["clazz"]
            service_instance = clazz(user=user, persistent=persistent)
            self.instance_map[name] = service_instance

        # build services with other dependencies
        for item in app_config["services_with_extra_deps"]:
            name, clazz = item["name"], item["clazz"]
            extra_depends = item.get("extra_deps", None)
            # seed depend_instances with the normal user and persistent objects
            depend_instances = [user, persistent]
            for s_name in extra_depends:
                depend_instances.append(self.instance_map[s_name])
            instance = clazz(*depend_instances)
            self.instance_map[name] = instance

        # build controllers
        for item in app_config["controllers"]:
            name, clazz = item["name"], item["clazz"]
            depends = item.get("depends", None)
            depend_instances = []
            for s_name in depends:
                depend_instances.append(self.instance_map[s_name])
            instance = clazz(*depend_instances)
            self.instance_map[name] = instance

    def __getattr__(self, key: str) -> Any:
        return self.instance_map[key]

    @classmethod
    def get_instance(
        cls,
        user: Any,
        persistent: Persistent,
        temp_storage: Storage,
        task_manager: AbstractTaskManager,
        storage: Storage,
    ) -> Any:
        """
        Get instance of AppContainer

        Parameters
        ----------
        user: User
            normal user
        persistent: Persistent
            persistent instance
        temp_storage: Storage
            temp storage
        task_manager: AbstractTaskManager
            task manager
        storage: Storage
            permanent storage

        Returns
        -------
        AppContainer instance
        """

        return AppContainer(
            user=user,
            persistent=persistent,
            temp_storage=temp_storage,
            task_manager=task_manager,
            storage=storage,
            app_config=app_container_config,
        )
