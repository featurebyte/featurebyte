"""
App container config module.

This contains all our registrations for dependency injection.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import inspect
import re
from dataclasses import dataclass

from featurebyte.enum import StrEnum

CAMEL_CASE_TO_SNAKE_CASE_PATTERN = re.compile(r"(?<!^)(?=[A-Z])")


def _get_class_name(class_name: str, name_override: Optional[str] = None) -> str:
    """
    Helper method to get a class name

    Parameters
    ----------
    class_name: type
        class
    name_override: str
        name override

    Returns
    -------
    str
        name
    """
    if name_override is not None:
        return name_override
    print(str(class_name))
    return CAMEL_CASE_TO_SNAKE_CASE_PATTERN.sub("_", class_name).lower()


def _get_constructor_params_from_class(
    class_: type, dependency_override: Optional[dict[str, str]] = None, skip_params: int = 0
) -> List[str]:
    """
    Helper method to get constructor params from class.

    Parameters
    ----------
    class_: type
        class

    Returns
    -------
    list[str]
        constructor params
    """
    sig = inspect.signature(class_.__init__)  # type: ignore[misc]
    keys = list(sig.parameters.keys())
    keys_to_skip = 1 + skip_params
    params = []
    dep_override = dependency_override or {}
    for key in keys[keys_to_skip:]:
        param_type = sig.parameters[key]
        type_annotation = param_type.name
        if isinstance(type_annotation, type):
            type_annotation = type_annotation.__name__
        class_name = _get_class_name(type_annotation)
        if key in dep_override:
            class_name = dep_override[key]
        params.append(class_name)
    return params


class DepType(StrEnum):
    """
    DepType enums.

    Used to determine what type of dependency we have, so that we can build them correctly.
    """

    SERVICE_WITH_EXTRA_DEPS = "service_with_extra_deps"
    CLASS_WITH_DEPS = "class_with_deps"


@dataclass
class ClassDefinition:
    """
    Basic class definition
    """

    # Note that we have a custom name, instead of using the name of the type directly.
    # This allows us to provide overrides to the names, and also allows us to better support multiple classes with
    # the same name.
    name: str
    class_: type
    dependencies: List[str]
    dep_type: DepType


class AppContainerConfig:
    """
    App container config holds all the dependencies for our application.
    """

    def __init__(self) -> None:
        # These services have dependencies in addition to the normal user, and persistent dependencies.
        self.service_with_extra_deps: List[ClassDefinition] = []
        # Classes with deps can depend on any object defined above.
        self.classes_with_deps: List[ClassDefinition] = []

        self.dependency_mapping: Dict[str, ClassDefinition] = {}

    def get_class_def_mapping(self) -> Dict[str, ClassDefinition]:
        """
        Get class definitions, keyed by name.
        """
        # Return if already populated
        if self.dependency_mapping:
            return self.dependency_mapping

        # Populate
        for dep in self._all_dependencies():
            self.dependency_mapping[dep.name] = dep
        return self.dependency_mapping

    def register_service(
        self,
        class_: type,
        dependency_override: Optional[Dict[str, str]] = None,
        name_override: Optional[str] = None,
    ) -> None:
        """
        Register a service with extra dependencies if needed.

        This endpoint is only for featurebyte services, as they'll automatically have a user, persistent, and catalog
        ID, injected into the service initialization.

        Parameters
        ----------
        class_: type
            type we are registering
        dependency_override: list[str]
            dependencies
        name_override: str
            name override
        """
        self.service_with_extra_deps.append(
            ClassDefinition(
                name=_get_class_name(class_.__name__, name_override),
                class_=class_,
                dependencies=_get_constructor_params_from_class(class_, dependency_override, 3),
                dep_type=DepType.SERVICE_WITH_EXTRA_DEPS,
            )
        )

    def register_class(
        self,
        class_: type,
        dependency_override: Optional[Dict[str, str]] = None,
        name_override: Optional[str] = None,
        force_no_deps: bool = False,
    ) -> None:
        """
        Register a class, with dependencies if needed.

        Parameters
        ----------
        class_: type
            type we are registering
        dependency_override: list[str]
            dependencies
        name_override: str
            name override
        force_no_deps: bool
            force no dependencies
        """
        deps = _get_constructor_params_from_class(class_, dependency_override)
        if force_no_deps:
            deps = []
        self.classes_with_deps.append(
            ClassDefinition(
                name=_get_class_name(class_.__name__, name_override),
                class_=class_,
                dependencies=deps,
                dep_type=DepType.CLASS_WITH_DEPS,
            )
        )

    def _all_dependencies(self) -> List[ClassDefinition]:
        output = []
        output.extend(self.service_with_extra_deps)
        output.extend(self.classes_with_deps)
        return output

    def _validate_duplicate_names(self) -> None:
        """
        Validate that there's no duplicate names registered.

        Raises
        ------
        ValueError
            raised when a name has been defined already.
        """
        seen_names = set()
        for definition in self._all_dependencies():
            definition_name = definition.name
            if definition_name in seen_names:
                raise ValueError(
                    f"error creating dependency map. {definition_name} has been defined already. "
                    "Consider changing the name of the dependency."
                )
            seen_names.add(definition_name)

    def _is_cyclic_dfs(
        self,
        class_def: ClassDefinition,
        visited_nodes: dict[str, bool],
        recursive_stack: dict[str, bool],
        class_def_mapping: dict[str, ClassDefinition],
    ) -> Tuple[bool, list[str]]:
        """
        DFS helper function to detect circular dependencies.

        Parameters
        ----------
        class_def: ClassDefinition
            class definition we are currently visiting
        visited_nodes: dict[str, bool]
            dictionary of visited nodes
        recursive_stack: dict[str, bool]
            dictionary of nodes currently in the recursive stack
        class_def_mapping: dict[str, ClassDefinition]
            dictionary of class definitions

        Returns
        -------
        bool
            True if there's a circular dependency, False otherwise.
        """
        # Mark current node as visited and adds to recursion stack.
        visited_nodes[class_def.name] = True
        recursive_stack[class_def.name] = True

        # Iterate through the dependencies
        # If any dependency has been visited before, and is in the current recursive stack, the
        # dependency graph is cyclic.
        for neighbour_name in class_def_mapping[class_def.name].dependencies:
            if neighbour_name not in class_def_mapping:
                print(class_def)
            neighbour = class_def_mapping[neighbour_name]
            if not visited_nodes.get(neighbour.name, False):
                is_cyclic, path = self._is_cyclic_dfs(
                    neighbour, visited_nodes, recursive_stack, class_def_mapping
                )
                if is_cyclic:
                    return True, path
            elif recursive_stack[neighbour.name]:
                cyclic_path = list(recursive_stack.keys())
                cyclic_path.append(neighbour.name)
                return True, cyclic_path

        # The node needs to be popped from stack before function ends
        recursive_stack[class_def.name] = False
        return False, []

    def _validate_circular_dependencies(self) -> None:
        """
        Validate that there are no circular dependencies.

        We do this by iterating through the graph dependencies in a DFS manner, and look for a back edge.

        Raises
        ------
        ValueError
            raised when a circular dependency is detected.
        """
        class_def_mapping = self.get_class_def_mapping()
        # Visited nodes keeps track of whether we have been to this node before.
        visited_nodes: dict[str, bool] = {}
        # Recursive stack keeps track of nodes that are currently being visited in the recursive call.
        # This is to allow us to see if there's a back edge.
        recursive_stack: dict[str, bool] = {}
        for node in self._all_dependencies():
            # Only need to recurse on nodes we have not been to before.
            if not visited_nodes.get(node.name, False):
                is_cyclic, path = self._is_cyclic_dfs(
                    node, visited_nodes, recursive_stack, class_def_mapping
                )
                if is_cyclic:
                    path_str = " -> ".join(path)
                    raise ValueError(
                        f"There's a circular dependency in the dependency graph.\n{path_str}"
                    )

    def validate(self) -> None:
        """
        Validate the correctness of the config.
        """
        self._validate_duplicate_names()
        self._validate_circular_dependencies()
