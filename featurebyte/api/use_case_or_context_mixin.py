"""
Base UseCase and Context mixin
"""

from typing import Any, Dict, List, Optional

import pandas as pd
from pydantic import Field
from typeguard import typechecked

from featurebyte.api.api_object import ApiObject
from featurebyte.api.api_object_util import iterate_api_object_using_paginated_routes
from featurebyte.api.observation_table import ObservationTable
from featurebyte.models.base import PydanticObjectId


class UseCaseOrContextMixin(ApiObject):
    """
    Base UseCase and Context mixin
    """

    default_preview_table_id: Optional[PydanticObjectId] = Field(default=None)
    default_eda_table_id: Optional[PydanticObjectId] = Field(default=None)

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
    def remove_observation_table(self, observation_table_name: str) -> None:
        """
        Remove observation table from the Use Case.

        Parameters
        ----------
        observation_table_name: str
            Name of new observation table to be removed.

        Examples
        --------
        >>> use_case = catalog.get_use_case("use_case")
        >>> use_case.remove_observation_table(observation_table_name)  # doctest: +SKIP
        """
        observation_table = ObservationTable.get(observation_table_name)
        self.update(
            update_payload={"observation_table_id_to_remove": observation_table.id},
            allow_update_local=False,
        )

    def remove_default_eda_table(self) -> None:
        """
        Remove observation table from the Use Case.

        Examples
        --------
        >>> use_case = catalog.get_use_case("use_case")
        >>> use_case.remove_default_eda_table()  # doctest: +SKIP
        """
        self.update(
            update_payload={"remove_default_eda_table": True, "default_eda_table_id": None},
            allow_update_local=False,
        )
        self.default_eda_table_id = None

    def remove_default_preview_table(self) -> None:
        """
        Remove observation table from the Use Case.

        Examples
        --------
        >>> use_case = catalog.get_use_case("use_case")
        >>> use_case.remove_default_preview_table()  # doctest: +SKIP
        """
        self.update(
            update_payload={"remove_default_preview_table": True, "default_preview_table_id": None},
            allow_update_local=False,
        )
        self.default_preview_table_id = None

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
