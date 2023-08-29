"""
UseCase module
"""
from __future__ import annotations

from typing import Any, List, Optional

import pandas as pd

from featurebyte.api.api_object_util import (
    ForeignKeyMapping,
    iterate_api_object_using_paginated_routes,
)
from featurebyte.api.observation_table import ObservationTable
from featurebyte.api.savable_api_object import DeletableApiObject, SavableApiObject
from featurebyte.api.target import Target
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.models.use_case import UseCaseModel
from featurebyte.schema.use_case import UseCaseCreate, UseCaseUpdate


class UseCase(SavableApiObject, DeletableApiObject):
    """
    UseCase class to represent a Use Case in FeatureByte.

    Users are encouraged to define Use Cases whilst creating features or feature_lists. Use Case formulates the
    modelling problem by associating a context with a target. In return, Users are better informed and more
    automation is offered to them.
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.UseCase")

    # class variables
    _route = "/use_case"
    _list_schema = UseCaseModel
    _get_schema = UseCaseModel
    _update_schema_class = UseCaseUpdate
    _list_fields = [
        "name",
        "target_name",
        "observation_table_ids",
        "default_preview_table_name",
        "default_eda_table_name",
        "description",
    ]

    _list_foreign_keys = [
        ForeignKeyMapping("target_id", Target, "target_name", "name"),
        ForeignKeyMapping(
            "default_preview_table_id", ObservationTable, "default_preview_table_name", "name"
        ),
        ForeignKeyMapping(
            "default_eda_table_id", ObservationTable, "default_eda_table_name", "name"
        ),
    ]

    # pydantic instance variable (public)
    target: Target
    # Context api object is not implemented yet. This is a placeholder.
    context: Optional[Any]

    def _get_create_payload(self) -> dict[str, Any]:
        """
        Get the payload for creating a new Use Case.

        Returns
        -------
        dict[str, Any]
        """
        data = UseCaseCreate(**self.dict(by_alias=True), target_id=self.target.id)
        return data.json_dict()

    def _get_init_params_from_object(self) -> dict[str, Any]:
        return {"target": self.target}

    @classmethod
    def create(
        cls,
        name: str,
        target: Target,
        context: Optional[Any] = None,
        description: Optional[str] = None,
    ) -> UseCase:
        """
        Create a new UseCase.

        Parameters
        ----------
        name: str
            Name of the UseCase.
        target: Target
            target object of the UseCase.
        context: Optional[Any]
            context of the UseCase.
        description: Optional[str]
            description of the UseCase.

        Returns
        -------
        UseCase
            The newly created UseCase.

        Examples
        --------
        >>> fb.UseCase.create(  # doctest: +SKIP
        ...     name="use_case_1",
        ...     context=context,
        ...     target=target,
        ...     description="use case description."
        ... )
        >>> use_case_1 = catalog.get_use_case("use_case_1")  # doctest: +SKIP
        """
        use_case = UseCase(name=name, target=target, context=context, description=description)
        use_case.save()
        return use_case

    def add_observation_table(self, new_observation_table: ObservationTable) -> None:
        """
        Add observation table for the Use Case.

        Parameters
        ----------
        new_observation_table: ObservationTable
            New observation table to be added.

        Examples
        --------
        >>> use_case = catalog.get_use_case("use_case")
        >>> use_case.add_observation_table(new_observation_table)  # doctest: +SKIP
        """
        self.update(
            update_payload={"new_observation_table_id": new_observation_table.id},
            allow_update_local=False,
        )

    def update_default_preview_table(self, default_preview_table: ObservationTable) -> None:
        """
        Update default preview table for the Use Case.

        Parameters
        ----------
        default_preview_table: ObservationTable
            New default preview table.

        Examples
        --------
        >>> use_case = catalog.get_use_case("use_case")
        >>> use_case.update_default_preview_table(default_preview_table)  # doctest: +SKIP
        """
        self.update(
            update_payload={"default_preview_table_id": default_preview_table.id},
            allow_update_local=False,
        )

    def update_default_eda_table(self, default_eda_table: ObservationTable) -> None:
        """
        Update default eda table for the Use Case.

        Parameters
        ----------
        default_eda_table: ObservationTable
            New default eda table.

        Examples
        --------
        >>> use_case = catalog.get_use_case("use_case")
        >>> use_case.update_default_eda_table(default_eda_table)  # doctest: +SKIP
        """
        self.update(
            update_payload={"default_eda_table_id": default_eda_table.id}, allow_update_local=False
        )

    def list_observation_tables(self) -> pd.DataFrame:
        """
        List observation tables associated with the Use Case.

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

    def list_feature_tables(self) -> pd.DataFrame:
        """
        List feature tables (BaseFeatureOrTargetTableModel) associated with the Use Case.

        Returns
        -------
        pd.DataFrame

        Examples
        --------
        >>> use_case = catalog.get_use_case("use_case")
        >>> use_case.list_feature_tables()  # doctest: +SKIP
        """
        route = f"{self._route}/{self.id}/feature_tables"
        return self._construct_table_result_df(
            list(iterate_api_object_using_paginated_routes(route))
        )

    def _construct_table_result_df(self, result_dict: List[dict[str, Any]]) -> pd.DataFrame:
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
        dataframe_dict: dict[str, Any] = {"id": [], "name": [], "description": []}
        for r_dict in result_dict:
            dataframe_dict["id"].append(r_dict["_id"])
            dataframe_dict["name"].append(r_dict["name"])
            dataframe_dict["description"].append(r_dict["description"])

        return pd.DataFrame.from_dict(dataframe_dict)
