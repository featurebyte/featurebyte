"""
Container for Controller objects to enable Dependency Injection
"""
from typing import Any, Dict

from featurebyte.persistent import Persistent
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
from featurebyte.routes.task.controller import TaskController
from featurebyte.routes.temp_data.controller import TempDataController
from featurebyte.service.default_version_mode import DefaultVersionModeService
from featurebyte.service.deploy import DeployService
from featurebyte.service.entity import EntityService
from featurebyte.service.event_data import EventDataService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_readiness import FeatureReadinessService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.online_enable import OnlineEnableService
from featurebyte.service.preview import PreviewService
from featurebyte.service.relationship import EntityRelationshipService
from featurebyte.service.task_manager import AbstractTaskManager
from featurebyte.service.version import VersionService
from featurebyte.storage import Storage

app_container_config = {
    "services": [
        {
            "name": "entity_service",
            "clazz": EntityService,
        },
        {
            "name": "event_data_service",
            "clazz": EventDataService,
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
            "name": "online_enable_service",
            "clazz": OnlineEnableService,
        },
        {
            "name": "deploy_service",
            "clazz": DeployService,
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
            "name": "default_version_mode_service",
            "clazz": DefaultVersionModeService,
        },
        {
            "name": "feature_store_service",
            "clazz": FeatureStoreService,
        },
        {
            "name": "preview_service",
            "clazz": PreviewService,
        },
        {
            "name": "version_service",
            "clazz": VersionService,
        },
        {
            "name": "entity_relationship_service",
            "clazz": EntityRelationshipService,
        },
    ],
    "controllers": [
        {
            "name": "entity_controller",
            "clazz": EntityController,
            "depends": [
                "entity_service",
                "entity_relationship_service",
            ],
        },
        {
            "name": "event_data_controller",
            "clazz": EventDataController,
            "depends": [
                "event_data_service",
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
            ],
        },
        {
            "name": "feature_job_setting_analysis_controller",
            "clazz": FeatureJobSettingAnalysisController,
            "depends": ["feature_job_setting_analysis_service", "task_controller"],
        },
        {
            "name": "feature_list_namespace_controller",
            "clazz": FeatureListNamespaceController,
            "depends": ["feature_list_namespace_service", "default_version_mode_service"],
        },
        {
            "name": "feature_namespace_controller",
            "clazz": FeatureNamespaceController,
            "depends": ["feature_namespace_service", "default_version_mode_service"],
        },
        {
            "name": "feature_store_controller",
            "clazz": FeatureStoreController,
            "depends": [
                "feature_store_service",
                "preview_service",
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

        for app_type in ["services", "controllers"]:
            for item in app_config[app_type]:
                name = item["name"]
                clazz = item["clazz"]
                depends = item.get("depends", None)
                if depends is None:
                    # construct service instances
                    instance = clazz(user=user, persistent=persistent)
                else:
                    # construct controller instances
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
