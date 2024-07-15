"""
UseCase module
"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Optional

import pandas as pd
from bson import ObjectId
from pandas import DataFrame
from typeguard import typechecked

from featurebyte.api.api_object_util import (
    ForeignKeyMapping,
    iterate_api_object_using_paginated_routes,
)
from featurebyte.api.context import Context
from featurebyte.api.observation_table import ObservationTable
from featurebyte.api.savable_api_object import DeletableApiObject, SavableApiObject
from featurebyte.api.target import Target
from featurebyte.api.target_namespace import TargetNamespace
from featurebyte.api.use_case_or_context_mixin import UseCaseOrContextMixin
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import ConflictResolution
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.use_case import UseCaseModel
from featurebyte.schema.use_case import UseCaseUpdate


class UseCase(SavableApiObject, DeletableApiObject, UseCaseOrContextMixin):
    """
    UseCase class to represent a Use Case in FeatureByte.

    Users are encouraged to define Use Cases whilst creating features or feature_lists. Use Case formulates the
    modelling problem by associating a context with a target. In return, Users are better informed and more
    automation is offered to them.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.UseCase")
    _route: ClassVar[str] = "/use_case"
    _list_schema: ClassVar[Any] = UseCaseModel
    _get_schema: ClassVar[Any] = UseCaseModel
    _update_schema_class: ClassVar[Any] = UseCaseUpdate
    _list_fields: ClassVar[List[str]] = [
        "name",
        "default_preview_table_name",
        "default_eda_table_name",
        "description",
    ]
    _list_foreign_keys: ClassVar[List[ForeignKeyMapping]] = [
        ForeignKeyMapping(
            "default_preview_table_id", ObservationTable, "default_preview_table_name", "name"
        ),
        ForeignKeyMapping(
            "default_eda_table_id", ObservationTable, "default_eda_table_name", "name"
        ),
    ]

    # pydantic instance variable (public)
    target_id: Optional[PydanticObjectId]
    target_namespace_id: PydanticObjectId
    context_id: PydanticObjectId

    @property
    def target(self) -> Optional[Target]:
        """
        Returns the target object of the UseCase.

        Returns
        -------
        Optional[Target]
            The target object of the UseCase.
        """
        if self.target_id is None:
            return None
        return Target.get_by_id(self.target_id)

    @property
    def context(self) -> Context:
        """
        Returns the context object of the UseCase.

        Returns
        -------
        Context
            The context object of the UseCase.
        """
        return Context.get_by_id(self.context_id)

    @classmethod
    @typechecked
    def create(
        cls,
        name: str,
        target_name: str,
        context_name: str,
        description: Optional[str] = None,
    ) -> UseCase:
        """
        Create a new UseCase.

        Parameters
        ----------
        name: str
            Name of the UseCase.
        target_name: str
            target name of the UseCase.
        context_name: str
            context name of the UseCase.
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
        ...     context_name="context_1",
        ...     target_name="target_1",
        ...     description="use case description.",
        ... )
        >>> use_case_1 = catalog.get_use_case("use_case_1")  # doctest: +SKIP
        """
        target_namespace = TargetNamespace.get(target_name)
        use_case = UseCase(
            name=name,
            target_id=target_namespace.default_target_id,
            target_namespace_id=target_namespace.id,
            context_id=Context.get(context_name).id,
            description=description,
        )
        use_case.save()
        return use_case

    @typechecked
    def add_observation_table(self, observation_table_name: str) -> None:
        """
        Add observation table for the Use Case.

        Parameters
        ----------
        observation_table_name: str
            Name of new observation table to be added.

        Examples
        --------
        >>> use_case = catalog.get_use_case("use_case")
        >>> use_case.add_observation_table(observation_table_name)  # doctest: +SKIP
        """
        observation_table = ObservationTable.get(observation_table_name)
        observation_table.update(
            update_payload={"use_case_id_to_add": self.id, "use_case_ids": None},
            allow_update_local=False,
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

    def list_deployments(self) -> pd.DataFrame:
        """
        List deployments associated with the Use Case.

        Returns
        -------
        pd.DataFrame

        Examples
        --------
        >>> use_case = catalog.get_use_case("use_case")
        >>> use_case.list_deployments()  # doctest: +SKIP
        """
        route = f"{self._route}/{self.id}/deployments"
        return self._construct_table_result_df(
            list(iterate_api_object_using_paginated_routes(route))
        )

    @typechecked
    def info(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Returns a dictionary that summarizes the essential information of a UseCase object. The dictionary
        contains the following keys:

        - `author`: The name of the user who created the UseCase object.
        - `name`: The name of the UseCase object.
        - `created_at`: The timestamp indicating when the UseCase object was created.
        - `updated_at`: The timestamp indicating when the UseCase object was last updated.
        - `primary_entities`: List of primary entities of the use case.
        - `description`: Description of the use case.
        - `default_eda_table`: Default eda table of the use case.
        - `default_preview_table`: Default preview table of the use case.

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
        >>> use_case = catalog.get_target("use_case")  # doctest: +SKIP
        >>> info = use_case.info()  # doctest: +SKIP
        """
        return super().info(verbose)

    @classmethod
    def get(cls, name: str) -> UseCase:
        """
        Gets a UseCase object by its name.

        Parameters
        ----------
        name: str
            Name of the deployment to retrieve.

        Returns
        -------
        UseCase
            UseCase object.

        Examples
        --------
        Get a UseCase object that is already saved.

        >>> use_case = fb.UseCase.get("use_case")  # doctest: +SKIP
        """
        return super().get(name)

    @classmethod
    def list(cls, include_id: Optional[bool] = True) -> DataFrame:
        """
        Returns a DataFrame that lists the use cases by their names, types and creation dates.

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
        List all use cases.

        >>> use_cases = fb.UseCase.list()
        """
        return super().list(include_id=include_id)

    @property
    def default_eda_table(self) -> Optional[ObservationTable]:
        """
        Returns the EDA table of the use case.

        Returns
        -------
        ObservationTable
            The ObservationTable object of the use case.

        Examples
        --------
        >>> use_case = catalog.get_use_case("use_case")  # doctest: +SKIP
        >>> use_case.default_eda_table  # doctest: +SKIP
        """
        return super().default_eda_table

    @property
    def default_preview_table(self) -> Optional[ObservationTable]:
        """
        Returns the preview table object of the use case.

        Returns
        -------
        ObservationTable
            The ObservationTable object of the use case.

        Examples
        --------
        >>> use_case = catalog.get_use_case("use_case")  # doctest: +SKIP
        >>> use_case.default_preview_table  # doctest: +SKIP
        """
        return super().default_preview_table

    @typechecked
    def update_default_preview_table(self, observation_table_name: str) -> None:
        """
        Update default preview table for the use case.

        Parameters
        ----------
        observation_table_name: str
            Name of default preview table.

        Examples
        --------
        >>> use_case = catalog.get_use_case("use_case")  # doctest: +SKIP
        >>> use_case.update_default_preview_table(observation_table_name)  # doctest: +SKIP
        """
        super().update_default_preview_table(observation_table_name)

    @typechecked
    def update_default_eda_table(self, observation_table_name: str) -> None:
        """
        Update default eda table for the use case.

        Parameters
        ----------
        observation_table_name: str
            Name of default eda table.

        Examples
        --------
        >>> use_case = catalog.get_use_case("use_case")  # doctest: +SKIP
        >>> use_case.update_default_eda_table(observation_table_name)  # doctest: +SKIP
        """
        super().update_default_eda_table(observation_table_name)

    @typechecked
    def update_description(self, description: Optional[str]) -> None:
        """
        Update description for the use case.

        Parameters
        ----------
        description: Optional[str]
            Description of the object

        Examples
        --------
        >>> use_case = catalog.get_use_case("use_case")  # doctest: +SKIP
        >>> use_case.update_description(description)  # doctest: +SKIP
        """
        super().update_description(description)

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
        super().remove_observation_table(observation_table_name)

    def remove_default_eda_table(self) -> None:
        """
        Remove observation table from the Use Case.

        Examples
        --------
        >>> use_case = catalog.get_use_case("use_case")
        >>> use_case.remove_default_eda_table()  # doctest: +SKIP
        """
        super().remove_default_eda_table()

    def remove_default_preview_table(self) -> None:
        """
        Remove observation table from the Use Case.

        Examples
        --------
        >>> use_case = catalog.get_use_case("use_case")
        >>> use_case.remove_default_preview_table()  # doctest: +SKIP
        """
        super().remove_default_preview_table()

    def list_observation_tables(self) -> pd.DataFrame:
        """
        List observation tables associated with the use case.

        Returns
        -------
        pd.DataFrame

        Examples
        --------
        >>> use_case = catalog.get_use_case("use_case")  # doctest: +SKIP
        >>> use_case.list_observation_tables()  # doctest: +SKIP
        """
        return super().list_observation_tables()

    @classmethod
    def get_by_id(cls, id: ObjectId) -> "UseCase":
        """
        Returns a UseCase object by its unique identifier (ID).

        Parameters
        ----------
        id: ObjectId
            UseCase unique identifier ID.

        Returns
        -------
        UseCase
            UseCase object.

        Examples
        --------
        Get a UseCase object that is already saved.

        >>> fb.UseCase.get_by_id(<use_case_id>)  # doctest: +SKIP
        """
        return cls._get_by_id(id=id)

    @typechecked
    def save(
        self, conflict_resolution: ConflictResolution = "raise", _id: Optional[ObjectId] = None
    ) -> None:
        """
        Adds a UseCase object to the catalog.

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
        >>> use_case = UseCase(
        ...     name="use_case", target_id=target_id, context_id=context_id
        ... )  # doctest: +SKIP
        >>> use_case.save()  # doctest: +SKIP
        """

        super().save(conflict_resolution=conflict_resolution, _id=_id)
