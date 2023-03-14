"""
App container config module.

This contains all our registrations for dependency injection.
"""
from typing import List

from dataclasses import dataclass


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


class AppContainerConfig:
    """
    App container config holds all the dependencies for our application.
    """

    def __init__(self) -> None:
        # These are objects which don't take in any dependencies, and can be instantiated as is.
        self.no_dependency_objects: List[ClassDefinition] = []
        # These services have dependencies in addition to the normal user, and persistent dependencies.
        self.service_with_extra_deps: List[ClassDefinition] = []
        # These services only require the user, and persistent dependencies.
        self.basic_services: List[ClassDefinition] = []
        # Controllers can depend on any object defined above.
        self.controllers: List[ClassDefinition] = []

    def add_no_dep_objects(self, name: str, class_: type) -> None:
        """
        Register a class with no dependencies.

        Parameters
        ----------
        name: str
            name of the object
        class_: type
            type we are registering
        """
        self.no_dependency_objects.append(
            ClassDefinition(name=name, class_=class_, dependencies=[])
        )

    def add_service_with_extra_deps(self, name: str, class_: type, dependencies: List[str]) -> None:
        """
        Register a service with extra dependencies

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
            )
        )

    def add_basic_service(self, name: str, class_: type) -> None:
        """
        Register a basic service

        Parameters
        ----------
        name: str
            name of the object
        class_: type
            type we are registering
        """
        self.basic_services.append(
            ClassDefinition(
                name=name,
                class_=class_,
                dependencies=[],
            )
        )

    def add_controller(self, name: str, class_: type, dependencies: List[str]) -> None:
        """
        Register a controller

        Parameters
        ----------
        name: str
            name of the object
        class_: type
            type we are registering
        dependencies: list[str]
            dependencies
        """
        self.controllers.append(
            ClassDefinition(
                name=name,
                class_=class_,
                dependencies=dependencies,
            )
        )

    def _all_dependencies(self) -> List[ClassDefinition]:
        output = []
        output.extend(self.no_dependency_objects)
        output.extend(self.basic_services)
        output.extend(self.service_with_extra_deps)
        output.extend(self.controllers)
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
