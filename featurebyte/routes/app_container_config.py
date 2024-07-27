"""
App container config module.

This contains all our registrations for dependency injection.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, get_type_hints

from featurebyte.models.base import CAMEL_CASE_TO_SNAKE_CASE_PATTERN

ARGS_AND_KWARGS = {"args", "kwargs"}


def _get_class_name(class_name: str, name_override: Optional[str] = None) -> str:
    """
    Helper method to get a class name.

    This method will convert a camel case formatted name, to a snake case formatted name.

    Examples:

    - `TestClass` -> `test_class`
    - 'SCDTable' -> 'scd_table'

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
    return CAMEL_CASE_TO_SNAKE_CASE_PATTERN.sub(r"_\1", class_name).lower()


def _get_constructor_params_from_class(
    class_: type, dependency_override: Optional[dict[str, str]] = None
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
    keys_to_skip = 1  # skip the `self` param
    params = []
    dep_override = dependency_override or {}
    for key in keys[keys_to_skip:]:
        # Skip args and kwargs parameters that appears in the default constructor.
        if key in ARGS_AND_KWARGS:
            continue
        param_type = sig.parameters[key]
        type_annotation = param_type.name
        if isinstance(type_annotation, type):
            type_annotation = type_annotation.__name__
        class_name = _get_class_name(type_annotation)
        if key in dep_override:
            class_name = dep_override[key]
        params.append(class_name)
    return params


@dataclass
class ClassDefinition:
    """
    Basic class definition
    """

    # Note that we have a custom name, instead of using the name of the type directly.
    # This allows us to provide overrides to the names, and also allows us to better support multiple classes with
    # the same name.
    name: str
    getter: Union[type, Callable[..., Any]]
    dependencies: List[str]


class AppContainerConfig:
    """
    App container config holds all the dependencies for our application.
    """

    def __init__(self) -> None:
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
        for dep in self.classes_with_deps:
            self.dependency_mapping[dep.name] = dep
        return self.dependency_mapping

    def register_factory_method(
        self, factory_method: Callable[..., Any], name_override: Optional[str] = None
    ) -> None:
        """
        Register a factory method. The name of the dependency will be based the name of the class that the factory
        method returns. For example, if the factory method returns a `TestClassA`, the name of the dependency will be
        `test_class_a`.

        Callers can use the name_override in a similar fashion to register_class if they want to provide a different
        name.

        Parameters
        ----------
        factory_method: Callable[..., Any]
            factory method
        name_override: str
            name override
        """
        name_to_use = (
            name_override
            if name_override is not None
            else _get_class_name(get_type_hints(factory_method)["return"].__name__)
        )
        self.classes_with_deps.append(
            ClassDefinition(
                name=name_to_use,
                getter=factory_method,
                dependencies=[],
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
        dependency_override: Optional[Dict[str, str]]
            We will normally look up dependencies by the name of the variable specified in the constructor of the
            class that we're registering. You can provide an override if you want to explicitly specify a class
            that we should inject instead. This is common when trying to initialize a class that inherits a constructor
            from a parent class, and the name in the constructor is something generic. For example, within our repo,
            simple controllers will typically inherit a constructor that has a parameter called "service". This will
            typically be the service that corresponds to the controller. However, since service is just a generic name,
            we can provide an override from `service` -> `controllers_service` to tell the dependency injector to
            look for `controllers_service` instead when trying to initialize this controller.
        name_override: str
            name override. The default name of this dependency is the class name, converted to snake case. If you
            want to override the name, provide a name here.
        force_no_deps: bool
            force no dependencies. This should only be used for instances that are directly injected into the
            instance_map, and are not constructed dynamically.
        """
        deps = _get_constructor_params_from_class(class_, dependency_override)
        if force_no_deps:
            deps = []
        self.classes_with_deps.append(
            ClassDefinition(
                name=_get_class_name(class_.__name__, name_override),
                getter=class_,
                dependencies=deps,
            )
        )

    def _validate_duplicate_names(self) -> None:
        """
        Validate that there's no duplicate names registered.

        Raises
        ------
        ValueError
            raised when a name has been defined already.
        """
        seen_names = set()
        for definition in self.classes_with_deps:
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

        Raises
        ------
        ValueError
        """
        # Mark current node as visited and adds to recursion stack.
        visited_nodes[class_def.name] = True
        recursive_stack[class_def.name] = True

        # Iterate through the dependencies
        # If any dependency has been visited before, and is in the current recursive stack, the
        # dependency graph is cyclic.
        for neighbour_name in class_def_mapping[class_def.name].dependencies:
            if neighbour_name not in class_def_mapping:
                raise ValueError(
                    f"Unable to find dependency {neighbour_name} in class_def_mappings for {class_def.name}. This is likely "
                    "because we have either not registered the dependency, or the variable name of "
                    "the dependency in the constructor isn't a snake case formatted name of the class "
                    "you are trying to inject."
                )
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
        for node in self.classes_with_deps:
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
