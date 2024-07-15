"""
Context module
"""

from typing import Any, ClassVar, Dict, List, Optional

import pandas as pd
from bson import ObjectId
from pandas import DataFrame
from typeguard import typechecked

from featurebyte.api.entity import Entity
from featurebyte.api.observation_table import ObservationTable
from featurebyte.api.savable_api_object import SavableApiObject
from featurebyte.api.use_case_or_context_mixin import UseCaseOrContextMixin
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import ConflictResolution
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.context import ContextModel
from featurebyte.schema.context import ContextUpdate


class Context(SavableApiObject, UseCaseOrContextMixin):
    """
    Context class to represent a Context in FeatureByte.

    A context defines the circumstances in which features are expected to be served.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.Context")
    _route: ClassVar[str] = "/context"
    _list_schema: ClassVar[Any] = ContextModel
    _get_schema: ClassVar[Any] = ContextModel
    _update_schema_class: ClassVar[Any] = ContextUpdate
    _list_fields: ClassVar[List[str]] = [
        "name",
        "primary_entity_ids",
        "description",
    ]

    # pydantic instance variable (public)
    primary_entity_ids: List[PydanticObjectId]

    @property
    def primary_entities(self) -> List[Entity]:
        """
        Returns the list of primary Entity objects from the context.

        Returns
        -------
        list[Entity]
            list of Primary Entity objects.

        Examples
        --------
        Get the primary enitity objects of the Context object:

        >>> context_1 = catalog.get_context("context_1")  # doctest: +SKIP
        >>> context_1.primary_entities  # doctest: +SKIP
        """
        entities = []
        for entity_id in self.primary_entity_ids:
            entities.append(Entity.get_by_id(entity_id))
        return entities

    @classmethod
    @typechecked
    def create(
        cls,
        name: str,
        primary_entity: List[str],
        description: Optional[str] = None,
    ) -> "Context":
        """
        Create a new Context.

        Parameters
        ----------
        name: str
            Name of the UseCase.
        primary_entity: List[str]
            List of entity names.
        description: Optional[str]
            Description of the Context.

        Returns
        -------
        Context
            The newly created Context.

        Examples
        --------
        >>> fb.Context.create(  # doctest: +SKIP
        ...     name="context_1",
        ...     primary_entity=primary_entity,
        ... )
        >>> context_1 = catalog.get_context("context_1")  # doctest: +SKIP
        """
        entity_ids = []
        for entity_name in primary_entity:
            entity_ids.append(Entity.get(entity_name).id)

        context = Context(name=name, primary_entity_ids=entity_ids, description=description)
        context.save()
        return context

    @typechecked
    def add_observation_table(self, observation_table_name: str) -> None:
        """
        Add observation table for the UseCase or Context.

        Parameters
        ----------
        observation_table_name: str
            New observation table to be added.

        Examples
        --------
        >>> context = catalog.get_context("context")
        >>> context.add_observation_table(observation_table_name)  # doctest: +SKIP
        """
        observation_table = ObservationTable.get(observation_table_name)
        observation_table.update(update_payload={"context_id": self.id}, allow_update_local=False)

    @typechecked
    def info(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Returns a dictionary that summarizes the essential information of a Context object. The dictionary
        contains the following keys:

        - `author`: The name of the user who created the Context object.
        - `name`: The name of the Context object.
        - `created_at`: The timestamp indicating when the Context object was created.
        - `updated_at`: The timestamp indicating when the Context object was last updated.
        - `primary_entities`: List of primary entities of the Context.
        - `description`: Description of the Context.
        - `default_eda_table`: Default eda table of the Context.
        - `default_preview_table`: Default preview table of the Context.
        - `associated_use_cases`: UseCases associated of the Context.

        Parameters
        ----------
        verbose: bool
            Control verbose level of the summary.

        Returns
        -------
        Dict[str, Any]
            Key-value mapping of properties of the object.

        Examples
        --------
        >>> context = catalog.get_target("context")  # doctest: +SKIP
        >>> info = context.info()  # doctest: +SKIP
        """
        return super().info(verbose)

    @classmethod
    def get(cls, name: str) -> "Context":
        """
        Gets a Context object by its name.

        Parameters
        ----------
        name: str
            Name of the deployment to retrieve.

        Returns
        -------
        Context
            Context object.

        Examples
        --------
        Get a Context object that is already saved.

        >>> context = fb.Context.get("context")  # doctest: +SKIP
        """
        return super().get(name)

    @classmethod
    def list(cls, include_id: Optional[bool] = True) -> DataFrame:
        """
        Returns a DataFrame that lists the contexts by their names, types and creation dates.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.

        Returns
        -------
        DataFrame
            Table of objects.

        Examples
        --------
        List all context.

        >>> contexts = fb.Context.list()
        """
        return super().list(include_id=include_id)

    @property
    def default_eda_table(self) -> Optional[ObservationTable]:
        """
        Returns the EDA table of the Context.

        Returns
        -------
        ObservationTable
            The ObservationTable object of the Context.
        """
        return super().default_eda_table

    @property
    def default_preview_table(self) -> Optional[ObservationTable]:
        """
        Returns the preview table object of the Context.

        Returns
        -------
        ObservationTable
            The ObservationTable object of the Context.
        """
        return super().default_preview_table

    @typechecked
    def update_default_preview_table(self, observation_table_name: str) -> None:
        """
        Update default preview table for the Context.

        Parameters
        ----------
        observation_table_name: str
            Name of default preview table.

        Examples
        --------
        >>> context = catalog.get_context("context")
        >>> context.update_default_preview_table(observation_table_name)  # doctest: +SKIP
        """
        super().update_default_preview_table(observation_table_name)

    @typechecked
    def update_default_eda_table(self, observation_table_name: str) -> None:
        """
        Update default eda table for the Context.

        Parameters
        ----------
        observation_table_name: str
            Name of default eda table.

        Examples
        --------
        >>> context = catalog.get_context("context")
        >>> context.update_default_eda_table(observation_table_name)  # doctest: +SKIP
        """
        super().update_default_eda_table(observation_table_name)

    @typechecked
    def update_description(self, description: Optional[str]) -> None:
        """
        Update description for the Context.

        Parameters
        ----------
        description: Optional[str]
            Description of the object

        Examples
        --------
        >>> context = catalog.get_context("context")
        >>> context.update_description(description)  # doctest: +SKIP
        """
        super().update_description(description)

    @typechecked
    def remove_observation_table(self, observation_table_name: str) -> None:
        """
        Remove observation table from the Context.

        Parameters
        ----------
        observation_table_name: str
            Name of new observation table to be removed.

        Examples
        --------
        >>> context = catalog.get_context("context")
        >>> context.remove_observation_table(observation_table_name)  # doctest: +SKIP
        """
        super().remove_observation_table(observation_table_name)

    def remove_default_eda_table(self) -> None:
        """
        Remove observation table from the Context.

        Examples
        --------
        >>> context = catalog.get_context("context")
        >>> context.remove_default_eda_table()  # doctest: +SKIP
        """
        super().remove_default_eda_table()

    def remove_default_preview_table(self) -> None:
        """
        Remove observation table from the Context.

        Examples
        --------
        >>> context = catalog.get_context("context")
        >>> context.remove_default_preview_table()  # doctest: +SKIP
        """
        super().remove_default_preview_table()

    def list_observation_tables(self) -> pd.DataFrame:
        """
        List observation tables associated with the Context.

        Returns
        -------
        pd.DataFrame

        Examples
        --------
        >>> context = catalog.get_context("context")
        >>> context.list_observation_tables()  # doctest: +SKIP
        """
        return super().list_observation_tables()

    @classmethod
    def get_by_id(cls, id: ObjectId) -> "Context":
        """
        Returns a Context object by its unique identifier (ID).

        Parameters
        ----------
        id: ObjectId
            Context unique identifier ID.

        Returns
        -------
        Context
            Context object.

        Examples
        --------
        Get a Context object that is already saved.

        >>> fb.Context.get_by_id(<context_id>)  # doctest: +SKIP
        """
        return cls._get_by_id(id=id)

    @typechecked
    def save(
        self, conflict_resolution: ConflictResolution = "raise", _id: Optional[ObjectId] = None
    ) -> None:
        """
        Adds a Context object to the catalog.

        A conflict could be triggered when the object being saved has violated a uniqueness check at the catalog.
        If uniqueness is violated, you can either raise an error or retrieve the object with the same name, depending
        on the conflict resolution parameter passed in. The default behavior is to raise an error.

        Parameters
        ----------
        conflict_resolution: ConflictResolution
            "raise" will raise an error when we encounter a conflict error.
            "retrieve" will handle the conflict error by retrieving the object with the same name.
        _id: Optional[ObjectId]
            The object ID to be used when saving the object. If not provided, a new object ID will be generated.

        Examples
        --------
        >>> context = Context(name=name, primary_entity_ids=entity_ids)  # doctest: +SKIP
        >>> context.save()  # doctest: +SKIP
        """

        super().save(conflict_resolution=conflict_resolution, _id=_id)
