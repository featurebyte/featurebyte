"""
Base UseCase and Context mixin
"""
from typing import Any, Dict, List, Optional

import pandas as pd
from typeguard import typechecked

from featurebyte.api.api_object import ApiObject
from featurebyte.api.api_object_util import iterate_api_object_using_paginated_routes
from featurebyte.api.observation_table import ObservationTable
from featurebyte.models.base import PydanticObjectId


class UseCaseOrContextMixin(ApiObject):
    """
    Base UseCase and Context mixin
    """

    default_preview_table_id: Optional[PydanticObjectId]
    default_eda_table_id: Optional[PydanticObjectId]

    @property
    def default_eda_table(self) -> Optional[ObservationTable]:
        """
        Returns the target object of the UseCase or Context.

        Returns
        -------
        ObservationTable
            The ObservationTable object of the UseCase or Context.
        """
        if self.default_eda_table_id:
            return ObservationTable.get_by_id(self.default_eda_table_id)

        return None

    @property
    def default_preview_table(self) -> Optional[ObservationTable]:
        """
        Returns the context object of the UseCase or Context.

        Returns
        -------
        ObservationTable
            The ObservationTable object of the UseCase or Context.
        """
        if self.default_preview_table_id:
            return ObservationTable.get_by_id(self.default_preview_table_id)

        return None

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
        >>> use_case = catalog.get_use_case("use_case")
        >>> use_case.add_observation_table(observation_table_name)  # doctest: +SKIP
        """
        observation_table = ObservationTable.get(observation_table_name)
        self._update_observation_table_context_id(observation_table)

    @typechecked
    def update_default_preview_table(self, observation_table_name: str) -> None:
        """
        Update default preview table for the UseCase or Context.

        Parameters
        ----------
        observation_table_name: str
            Name of default preview table.

        Examples
        --------
        >>> use_case = catalog.get_use_case("use_case")
        >>> use_case.update_default_preview_table(observation_table_name)  # doctest: +SKIP
        """
        observation_table = ObservationTable.get(observation_table_name)
        self._update_observation_table_context_id(observation_table)

        self.update(
            update_payload={"default_preview_table_id": observation_table.id},
            allow_update_local=False,
        )
        self.default_preview_table_id = observation_table.id

    @typechecked
    def update_default_eda_table(self, observation_table_name: str) -> None:
        """
        Update default eda table for the UseCase or Context.

        Parameters
        ----------
        observation_table_name: str
            Name of default eda table.

        Examples
        --------
        >>> use_case = catalog.get_use_case("use_case")
        >>> use_case.update_default_eda_table(observation_table_name)  # doctest: +SKIP
        """
        observation_table = ObservationTable.get(observation_table_name)
        self._update_observation_table_context_id(observation_table)

        self.update(
            update_payload={"default_eda_table_id": observation_table.id}, allow_update_local=False
        )
        self.default_eda_table_id = observation_table.id

    def list_observation_tables(self) -> pd.DataFrame:
        """
        List observation tables associated with the UseCase or Context.

        Returns
        -------
        pd.DataFrame

        Examples
        --------
        >>> use_case = catalog.get_use_case("use_case")
        >>> use_case.list_observation_tables()  # doctest: +SKIP
        """
        route = f"{self._route}/{self.id}/observation_tables"
        return self._construct_table_result_df(
            list(iterate_api_object_using_paginated_routes(route))
        )

    @typechecked
    def _construct_table_result_df(self, result_dict: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Construct a pandas DataFrame from the result_dict returned by the API.

        Parameters
        ----------
        result_dict: dict[str, Any]
            The result_dict returned by the API.

        Returns
        -------
        pd.DataFrame
            The pandas DataFrame constructed from the result_dict.
        """
        dataframe_dict: Dict[str, Any] = {"id": [], "name": [], "description": []}
        for r_dict in result_dict:
            dataframe_dict["id"].append(r_dict["_id"])
            dataframe_dict["name"].append(r_dict["name"])
            dataframe_dict["description"].append(r_dict["description"])

        return pd.DataFrame.from_dict(dataframe_dict)

    @typechecked
    def _update_observation_table_context_id(self, observation_table: ObservationTable) -> None:
        """
        Update the context_id of the ObservationTable object.

        Parameters
        ----------
        observation_table: ObservationTable
            The ObservationTable object to be updated.
        """
        observation_table.update(update_payload={"context_id": self.id}, allow_update_local=False)
