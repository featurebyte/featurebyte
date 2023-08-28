"""
UseCase module
"""
from __future__ import annotations

from typing import Any, List, Optional

import pandas as pd
from bson import ObjectId

from featurebyte.api.api_object_util import (
    ForeignKeyMapping,
    get_api_object_by_id,
    iterate_api_object_using_paginated_routes,
)
from featurebyte.api.observation_table import ObservationTable
from featurebyte.api.savable_api_object import DeletableApiObject, SavableApiObject
from featurebyte.api.target import Target
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.utils import dataframe_from_json
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.use_case import UseCaseModel
from featurebyte.schema.use_case import UseCaseCreate, UseCaseRead, UseCaseUpdate


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
            "default_eda_table__d", ObservationTable, "default_eda_table_name", "name"
        ),
    ]

    # pydantic instance variable (public)
    target: Target

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
        description: Optional[str] = None,
    ) -> UseCase:
        """
        Create a new Use Case.

        Parameters
        ----------
        name: str
            Name of the UseCase.
        target: Target
            target object of the UseCase
        description: Optional[str]
            description of the UseCase.

        Returns
        -------
        UseCase
            The newly created Use Case.

        Examples
        --------
        """
        use_case = UseCase(name=name, target=target, description=description)
        use_case.save()
        return use_case

    def add_observation_table(self, new_observation_table_id: ObjectId) -> None:
        """
        Add observation table for the Use Case.

        Examples
        --------
        """
        self.update(
            update_payload={"new_observation_table_id": new_observation_table_id},
            allow_update_local=False,
        )

    def update_default_preview_table(self, default_preview_table_id: ObjectId) -> None:
        """
        Update default preview table for the Use Case.

        Examples
        --------
        """
        self.update(
            update_payload={"default_preview_table_id": default_preview_table_id},
            allow_update_local=False,
        )

    def update_default_eda_table(self, default_eda_table_id: ObjectId) -> None:
        """
        Update default eda table for the Use Case.

        Examples
        --------
        """
        self.update(
            update_payload={"default_eda_table_id": default_eda_table_id}, allow_update_local=False
        )

    def list_observation_tables(self) -> List[ObservationTableModel]:
        """
        List observation tables associated with the Use Case.

        Examples
        --------
        """
        route = f"{self._route}/{self.id}/observation_tables"
        feature_tables = []
        for feature_table_dict in iterate_api_object_using_paginated_routes(route):
            feature_tables.append(ObservationTableModel(**feature_table_dict))
        return feature_tables

    def list_feature_tables(self) -> pd.DataFrame:
        """
        List feature tables (BaseFeatureOrTargetTableModel) associated with the Use Case.

        Examples
        --------
        """
        result = get_api_object_by_id(self._route, self.id, "feature_tables")
        obs_tables = dataframe_from_json(result)

        return obs_tables

    def list_deployments(self) -> None:
        """
        List deployments associated with the Use Case

        Examples
        --------
        """
        raise NotImplementedError
