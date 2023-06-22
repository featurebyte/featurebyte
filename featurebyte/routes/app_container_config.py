"""
App container config module.

This contains all our registrations for dependency injection.
"""
from typing import Dict, List

from dataclasses import dataclass

from featurebyte.enum import StrEnum


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

    def register_service(self, name: str, class_: type, dependencies: List[str] = ()) -> None:
        """
        Register a service with extra dependencies if needed.

        This endpoint is only for featurebyte services, as they'll automatically have a user, persistent, and catalog
        ID, injected into the service initialization.

        Parameters
        ----------
        name: str
            name of the object
        class_: type
            type we are registering
        dependencies: list[str]
            dependencies
        """
        self.service_with_extra_deps.append(
            ClassDefinition(
                name=name,
                class_=class_,
                dependencies=dependencies,
                dep_type=DepType.SERVICE_WITH_EXTRA_DEPS,
            )
        )

    def register_class(self, name: str, class_: type, dependencies: List[str] = ()) -> None:
        """
        Register a class, with dependencies if needed.

        Parameters
        ----------
        name: str
            name of the object
        class_: type
            type we are registering
        dependencies: list[str]
            dependencies
        """
        self.classes_with_deps.append(
            ClassDefinition(
                name=name,
                class_=class_,
                dependencies=dependencies,
                dep_type=DepType.CLASS_WITH_DEPS,
            )
        )

    def _all_dependencies(self) -> List[ClassDefinition]:
        output = []
        output.extend(self.service_with_extra_deps)
        output.extend(self.classes_with_deps)
        return output

    def validate(self) -> None:
        """
        Validate the correctness of the config. We check that there's no duplicate names registered.
        Can consider pushing this into each of the add functions so we can fail faster.

        Raises
        ------
        ValueError
            raised when a name has been defined already.
        """
        # validate that there are no clashing names
        seen_names = set()
        for definition in self._all_dependencies():
            definition_name = definition.name
            if definition_name in seen_names:
                raise ValueError(
                    f"error creating dependency map. {definition_name} has been defined already. "
                    "Consider changing the name of the dependency."
                )
            seen_names.add(definition_name)
