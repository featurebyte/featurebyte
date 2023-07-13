"""
Lazy app container functions the same as the app_container, but only initializes dependencies when needed.
"""
from typing import Any, Dict, List, Optional

from bson import ObjectId
from celery import Celery

from featurebyte.persistent import Persistent
from featurebyte.routes.app_container_config import AppContainerConfig, ClassDefinition
from featurebyte.storage import Storage


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
) -> Dict[str, Any]:
    """
    Build dependencies for a given list of class definitions.

    Parameters
    ----------
    deps: List[ClassDefinition]
        list of class definitions
    existing_deps: Dict[str, Any]
        mapping of key to instance

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
        celery: Celery,
        storage: Storage,
        catalog_id: Optional[ObjectId],
        app_container_config: AppContainerConfig,
    ):
        self.app_container_config = app_container_config

        # Used to cache instances if they've already been built
        # Pre-load with some default deps
        self.instance_map: Dict[str, Any] = {
            "catalog_id": catalog_id,
            "celery": celery,
            "persistent": persistent,
            "storage": storage,
            "temp_storage": temp_storage,
            "user": user,
        }

    def _get_key(self, key: str) -> Any:
        """
        Helper method to get a key from the instance map.

        Parameters
        ----------
        key: str
            key to get from the instance map

        Returns
        -------
        Any
        """
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
        new_deps = build_deps(ordered_deps, self.instance_map)
        self.instance_map.update(new_deps)
        return self.instance_map[key]

    def get(self, key: str) -> Any:
        """
        Get an instance from the container.

        Parameters
        ----------
        key: str
            key of the instance to get

        Returns
        -------
        Any
        """
        return self._get_key(key)

    def __getattr__(self, key: str) -> Any:
        return self._get_key(key)
