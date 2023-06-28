"""
Lazy app container functions the same as the app_container, but only initializes dependencies when needed.
"""
from typing import Any, Dict, List

from bson import ObjectId

from featurebyte.persistent import Persistent
from featurebyte.routes.app_container_config import AppContainerConfig, ClassDefinition, DepType
from featurebyte.routes.task.controller import TaskController
from featurebyte.routes.temp_data.controller import TempDataController
from featurebyte.service.task_manager import TaskManager
from featurebyte.storage import Storage
from featurebyte.utils.credential import MongoBackedCredentialProvider


def get_all_deps_for_key(
    key: str, class_def_mapping: Dict[str, ClassDefinition], base_deps: Dict[str, Any]
) -> List[str]:
    """
    Get dependencies for a given key.

    Parameters
    ----------
    key: str
        key to get dependencies for
    class_def_mapping: Dict[str, ClassDefinition]
        mapping of key to class definition
    base_deps: Dict[str, Any]
        existing dependencies

    Returns
    -------
    Dict[str, ClassDefinition]
        ordered dependencies, with the elements
    """
    all_deps = [key]
    # Skip if the key is already in the base_deps
    if key in base_deps:
        return all_deps

    # Get class definition
    class_def = class_def_mapping[key]
    dependencies = class_def.dependencies
    # If this node has no children, return the current node.
    if not dependencies:
        return all_deps

    # Recursively get all dependencies of the children
    for dep in dependencies:
        # Can skip if the dependency has been traversed before already.
        if dep in all_deps:
            continue
        children_deps = get_all_deps_for_key(dep, class_def_mapping, base_deps)
        for current_all_dep in all_deps:
            if current_all_dep in children_deps:
                continue
            children_deps.append(current_all_dep)
        all_deps = children_deps

    return all_deps


def build_service_with_deps(
    class_def: ClassDefinition,
    user: Any,
    persistent: Persistent,
    catalog_id: ObjectId,
    instance_map: Dict[str, Any],
) -> Any:
    """
    Build a service with the given dependencies.

    Parameters
    ----------
    class_def: ClassDefinition
        class definition
    user: Any
        user object
    persistent: Persistent
        persistent object
    catalog_id: ObjectId
        catalog id
    instance_map: Dict[str, Any]
        mapping of key to instance

    Returns
    -------
    Any
    """
    extra_depends = class_def.dependencies
    # Seed depend_instances with the normal user and persistent objects
    depend_instances = [user, persistent, catalog_id]
    for s_name in extra_depends:
        depend_instances.append(instance_map[s_name])
    return class_def.class_(*depend_instances)


def build_class_with_deps(class_definition: ClassDefinition, instance_map: Dict[str, Any]) -> Any:
    """
    Build a class with the given dependencies.

    Parameters
    ----------
    class_definition: ClassDefinition
        class definition
    instance_map: Dict[str, Any]
        mapping of key to instance

    Returns
    -------
    Any
    """
    depends = class_definition.dependencies
    depend_instances = []
    for s_name in depends:
        depend_instances.append(instance_map[s_name])
    return class_definition.class_(*depend_instances)


def build_deps(
    deps: List[ClassDefinition],
    existing_deps: Dict[str, Any],
    user: Any,
    persistent: Persistent,
    catalog_id: ObjectId,
) -> Dict[str, Any]:
    """
    Build dependencies for a given list of class definitions.

    Parameters
    ----------
    deps: List[ClassDefinition]
        list of class definitions
    existing_deps: Dict[str, Any]
        mapping of key to instance
    user: Any
        user object
    persistent: Persistent
        persistent object
    catalog_id: ObjectId
        catalog id

    Returns
    -------
    Dict[str, Any]
    """
    # Build deps
    new_deps = {}
    new_deps.update(existing_deps)
    for dep in deps:
        # Skip if built already
        if dep.name in new_deps:
            continue
        # Build dependencies for this dep
        if dep.dep_type == DepType.SERVICE_WITH_EXTRA_DEPS:
            new_deps[dep.name] = build_service_with_deps(
                dep, user, persistent, catalog_id, new_deps
            )
        elif dep.dep_type == DepType.CLASS_WITH_DEPS:
            new_deps[dep.name] = build_class_with_deps(dep, new_deps)
    return new_deps


def convert_dep_list_str_to_class_def(
    deps: List[str], mapping: Dict[str, ClassDefinition]
) -> List[ClassDefinition]:
    """
    Converts dependencies from a list of strings to a list of ClassDefinitions.

    Parameters
    ----------
    deps: List[str]
        list of dependencies
    mapping: Dict[str, ClassDefinition]
        mapping of key to class definition

    Returns
    -------
    List[ClassDefinition[
    """
    output: List[ClassDefinition] = []
    for dep in deps:
        output.append(mapping[dep])
    return output


class LazyAppContainer:
    """
    LazyAppContainer is a container for all the services and controllers used in the app.

    We only initialize the dependencies that are needed for a given request as invoked by __getattr__.
    """

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        temp_storage: Storage,
        task_manager: TaskManager,
        storage: Storage,
        catalog_id: ObjectId,
        app_container_config: AppContainerConfig,
    ):
        self.user = user
        self.persistent = persistent
        self.temp_storage = temp_storage
        self.task_manager = task_manager
        self.storage = storage
        self.catalog_id = catalog_id
        self.app_container_config = app_container_config

        # Used to cache instances if they've already been built
        # Pre-load with some default deps
        self.instance_map: Dict[str, Any] = {
            "task_controller": TaskController(task_manager=task_manager),
            "temp_data_controller": TempDataController(temp_storage=temp_storage),
            "mongo_backed_credential_provider": MongoBackedCredentialProvider(
                persistent=persistent
            ),
            "task_manager": task_manager,
            "persistent": persistent,
        }

    def _get_key(self, key: str) -> Any:
        # Return instance if it's been built before already
        if key in self.instance_map:
            return self.instance_map[key]

        # Get deps by doing a depth first traversal through the dependencies
        deps = get_all_deps_for_key(
            key, self.app_container_config.get_class_def_mapping(), self.instance_map
        )
        # Remove deps that have already been built
        filtered_deps = [dep for dep in deps if dep not in self.instance_map]
        ordered_deps = convert_dep_list_str_to_class_def(
            filtered_deps, self.app_container_config.get_class_def_mapping()
        )
        new_deps = build_deps(
            ordered_deps, self.instance_map, self.user, self.persistent, self.catalog_id
        )
        self.instance_map.update(new_deps)
        return self.instance_map[key]

    def get(self, key: str) -> Any:
        return self._get_key(key)

    def __getattr__(self, key: str) -> Any:
        return self._get_key(key)
