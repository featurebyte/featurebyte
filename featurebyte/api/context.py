"""
Context module
"""
from typing import Any, Dict, List, Optional

import pandas as pd
from pandas import DataFrame
from typeguard import typechecked

from featurebyte.api.entity import Entity
from featurebyte.api.observation_table import ObservationTable
from featurebyte.api.savable_api_object import SavableApiObject
from featurebyte.api.use_case_or_context_mixin import UseCaseOrContextMixin
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.context import ContextModel
from featurebyte.schema.context import ContextUpdate


class Context(SavableApiObject, UseCaseOrContextMixin):
    """
    Context class to represent a Context in FeatureByte.

    A context defines the circumstances in which features are expected to be served.
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.Context")

    # class variables
    _route = "/context"
    _list_schema = ContextModel
    _get_schema = ContextModel
    _update_schema_class = ContextUpdate
    _list_fields = [
        "name",
        "entity_ids",
        "description",
    ]

    # pydantic instance variable (public)
    entity_ids: List[PydanticObjectId]

    @property
    def entities(self) -> List[Entity]:
        """
        Returns the history of the entity name of the Entity object.

        Returns
        -------
        list[Entity]
            list of Entity objects.

        Examples
        --------
        Get the enitity objects of the Context object:

        >>> context_1 = catalog.get_context("context_1")  # doctest: +SKIP
        >>> context_1.entities  # doctest: +SKIP
        """
        entities = []
        for entity_id in self.entity_ids:
            entities.append(Entity.get_by_id(entity_id))
        return entities

    @classmethod
    @typechecked
    def create(
        cls,
        name: str,
        entity_names: List[str],
    ) -> "Context":
        """
        Create a new Context.

        Parameters
        ----------
        name: str
            Name of the UseCase.
        entity_names: List[str]
            List of entity names.

        Returns
        -------
        Context
            The newly created Context.

        Examples
        --------
        >>> fb.Context.create(  # doctest: +SKIP
        ...     name="context_1",
        ...     entity_names=entity_names,
        ... )
        >>> context_1 = catalog.get_context("context_1")  # doctest: +SKIP
        """
        entity_ids = []
        for entity_name in entity_names:
            entity_ids.append(Entity.get(entity_name).id)

        context = Context(name=name, entity_ids=entity_ids)
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
        - `entities`: List of primary entities of the Context.
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
