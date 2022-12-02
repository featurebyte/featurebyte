"""
Container for Controller objects to enable Dependency Injection
"""
from typing import Any, Dict, List

from featurebyte.persistent import Persistent
from featurebyte.routes.task.controller import TaskController
from featurebyte.routes.temp_data.controller import TempDataController
from featurebyte.service.task_manager import AbstractTaskManager
from featurebyte.storage import Storage

app_container_config = {
    "services": [],
    "controllers": [],
}


def register_service_constructor(clazz: Any):
    classes = app_container_config["services"]
    classes.extend(
        [
            {
                "name": clazz.__name__,
                "clazz": clazz,
            }
        ]
    )
    app_container_config["services"] = classes


def register_controller_constructor(clazz: Any, dependencies: List[Any]) -> None:
    controllers = app_container_config["controllers"]
    controllers.extend(
        [
            {
                "name": clazz.__name__,
                "clazz": clazz,
                "depends": dependencies,
            }
        ]
    )
    app_container_config["controllers"] = controllers


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
        print(self.instance_map)

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
