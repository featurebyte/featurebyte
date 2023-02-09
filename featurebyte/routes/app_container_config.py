"""
App container config module.

This contains all our registrations for dependency injection.
"""
from typing import List

from dataclasses import dataclass


@dataclass
class ClazzDefinition:
    """
    Basic clazz definition
    """

    # Note that we have a custom name, instead of using the name of the type directly.
    # This allows us to provide overrides to the names, and also allows us to better support multiple classes with
    # the same name.
    name: str
    clazz: type


@dataclass
class ClazzDefinitionWithDependencies(ClazzDefinition):
    """
    Clazz definition with dependencies
    """

    dependencies: List[str]


class AppContainerConfig:
    """
    App container config holds all the dependencies for our application.
    """

    def __init__(self) -> None:
        # These are objects which don't take in any dependencies, and can be instantiated as is.
        self.no_dependency_objects: List[ClazzDefinition] = []
        # These services have dependencies in addition to the normal user, and persistent dependencies.
        self.service_with_extra_deps: List[ClazzDefinitionWithDependencies] = []
        # These services only require the user, and persistent dependencies.
        self.basic_services: List[ClazzDefinition] = []
        # Controllers can depend on any object defined above.
        self.controllers: List[ClazzDefinitionWithDependencies] = []

    def get_no_dep_objects(self) -> List[ClazzDefinition]:
        """
        Get clazz definitions with no dependencies.

        Returns
        -------
        list[ClazzDefinition]
            classes that have no dependencies
        """
        return self.no_dependency_objects

    def add_no_dep_objects(self, name: str, clazz: type) -> None:
        """
        Register a class with no dependencies.

        Parameters
        ----------
        name: str
            name of the object
        clazz: type
            type we are registering
        """
        self.no_dependency_objects.append(ClazzDefinition(name=name, clazz=clazz))

    def get_services_with_extra_deps(self) -> List[ClazzDefinitionWithDependencies]:
        """
        Get services definitions with extra dependencies.

        Returns
        -------
        list[ClazzDefinitionWithDependencies]
            services that have extra dependencies
        """
        return self.service_with_extra_deps

    def add_service_with_extra_deps(self, name: str, clazz: type, dependencies: List[str]) -> None:
        """
        Register a service with extra dependencies

        Parameters
        ----------
        name: str
            name of the object
        clazz: type
            type we are registering
        dependencies: list[str]
            dependencies
        """
        self.service_with_extra_deps.append(
            ClazzDefinitionWithDependencies(
                name=name,
                clazz=clazz,
                dependencies=dependencies,
            )
        )

    def get_basic_services(self) -> List[ClazzDefinition]:
        """
        Get services definitions with no extra dependencies.

        Returns
        -------
        list[ClazzDefinition]
            services that have no extra dependencies, apart from user and persistent
        """
        return self.basic_services

    def add_basic_service(self, name: str, clazz: type) -> None:
        """
        Register a basic service

        Parameters
        ----------
        name: str
            name of the object
        clazz: type
            type we are registering
        """
        self.basic_services.append(
            ClazzDefinition(
                name=name,
                clazz=clazz,
            )
        )

    def get_controllers(self) -> List[ClazzDefinitionWithDependencies]:
        """
        Get controllers.

        Returns
        -------
        list[ClazzDefinitionWithDependencies]
            controllers
        """
        return self.controllers

    def add_controller(self, name: str, clazz: type, dependencies: List[str]) -> None:
        """
        Register a controller

        Parameters
        ----------
        name: str
            name of the object
        clazz: type
            type we are registering
        dependencies: list[str]
            dependencies
        """
        self.controllers.append(
            ClazzDefinitionWithDependencies(
                name=name,
                clazz=clazz,
                dependencies=dependencies,
            )
        )

    def _all_dependencies(self) -> List[ClazzDefinition]:
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
