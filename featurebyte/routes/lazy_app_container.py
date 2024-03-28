"""
Lazy app container functions the same as the app_container, but only initializes dependencies when needed.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Type, Union

from bson import ObjectId
from celery import Celery
from redis.client import Redis

from featurebyte.persistent import Persistent
from featurebyte.routes.app_container_config import (
    AppContainerConfig,
    ClassDefinition,
    _get_class_name,
)
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
    getter = class_definition.getter
    # If the getter is a class constructor, call it with the dependencies.
    if isinstance(getter, type):
        return getter(*depend_instances)
    # If not, we assume it's a factory method without any deps. Thus, we can just construct it directly.
    return getter()


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
        app_container_config: AppContainerConfig,
        user: Optional[Any] = None,
        temp_storage: Optional[Storage] = None,
        celery: Optional[Celery] = None,
        redis: Optional[Redis[Any]] = None,
        storage: Optional[Storage] = None,
        catalog_id: Optional[ObjectId] = None,
        persistent: Optional[Persistent] = None,
        instance_map: Optional[Dict[str, Any]] = None,
    ):
        self.app_container_config = app_container_config

        # Used to cache instances if they've already been built.
        # Pre-load with some default deps if they're not provided.
        self.instance_map: Dict[str, Any] = (
            {
                "catalog_id": catalog_id,
                "celery": celery,
                "redis": redis,
                "persistent": persistent,
                "storage": storage,
                "temp_storage": temp_storage,
                "user": user,
            }
            if instance_map is None
            else instance_map
        )

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

    @staticmethod
    def _get_key_to_use(key: Union[str, Type[Any]]) -> str:
        if isinstance(key, str):
            return key
        return _get_class_name(key.__name__)

    def get(self, key: Union[str, Type[Any]]) -> Any:
        """
        Get an instance from the container.

        Parameters
        ----------
        key: Union[str, Type[Any]]
            key of the instance to get, or the type of the instance

        Returns
        -------
        Any
        """
        key_to_use = self._get_key_to_use(key)
        return self._get_key(key_to_use)

    def override_instance_for_test(self, key: str, instance: Any) -> None:
        """
        Override an instance for testing purposes.

        Parameters
        ----------
        key: str
            key to override
        instance: Any
            instance to override with
        """
        self.override_instances_for_test({key: instance})

    def override_instances_for_test(self, instances_to_update: Dict[str, Any]) -> None:
        """
        Override multiple instances for testing purposes.

        Parameters
        ----------
        instances_to_update: Dict[str, Any]
            mapping of key to instance
        """
        self.instance_map.update(instances_to_update)

    def invalidate_dep_for_test(self, key: Union[str, Type[Any]]) -> None:
        """
        Invalidate a dependency for testing purposes. This will remove the dependency from the instance map, and
        force the dep to be re-created when it's next invoked. This is useful after we have overridden an instance
        for test.

        Parameters
        ----------
        key: Union[str, Type[Any]]
            key of the instance to invalidate, or the type of the instance
        """
        key_to_use = self._get_key_to_use(key)
        if key_to_use in self.instance_map:
            del self.instance_map[key_to_use]

    def __getattr__(self, key: str) -> Any:
        return self._get_key(key)
