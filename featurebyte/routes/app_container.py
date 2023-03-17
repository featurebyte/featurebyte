"""
Container for Controller objects to enable Dependency Injection
"""
from typing import Any, Dict

from bson import ObjectId

from featurebyte.persistent import Persistent
from featurebyte.routes.app_container_config import AppContainerConfig
from featurebyte.routes.registry import app_container_config
from featurebyte.routes.task.controller import TaskController
from featurebyte.routes.temp_data.controller import TempDataController
from featurebyte.service.task_manager import AbstractTaskManager
from featurebyte.storage import Storage


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
        app_config: AppContainerConfig,
        workspace_id: ObjectId,
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
        workspace_id: ObjectId
            workspace id
        """
        _ = storage  # not used in the app_container bean yet

        app_config.validate()

        self.instance_map: Dict[str, Any] = {
            "task_controller": TaskController(task_manager=task_manager),
            "tempdata_controller": TempDataController(temp_storage=temp_storage),
        }

        # build no dependency objects
        for item in app_config.no_dependency_objects:
            self.instance_map[item.name] = item.class_()

        # build services
        for item in app_config.basic_services:
            name, class_ = item.name, item.class_
            service_instance = class_(user=user, persistent=persistent, workspace_id=workspace_id)
            self.instance_map[name] = service_instance

        # build services with other dependencies
        for item in app_config.service_with_extra_deps:
            name, class_ = item.name, item.class_
            extra_depends = item.dependencies
            # seed depend_instances with the normal user and persistent objects
            depend_instances = [user, persistent, workspace_id]
            for s_name in extra_depends:
                depend_instances.append(self.instance_map[s_name])
            instance = class_(*depend_instances)
            self.instance_map[name] = instance

        # build controllers
        for item in app_config.controllers:
            name, class_ = item.name, item.class_
            depends = item.dependencies
            depend_instances = []
            for s_name in depends:
                depend_instances.append(self.instance_map[s_name])
            instance = class_(*depend_instances)
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
        workspace_id: ObjectId,
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
        workspace_id: ObjectId
            workspace id

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
            workspace_id=workspace_id,
        )
