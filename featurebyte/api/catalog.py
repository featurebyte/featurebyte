"""
Catalog module
"""
from typing import Optional

import pandas as pd
from bson import ObjectId
from typeguard import typechecked

from featurebyte import (
    Data,
    DimensionData,
    Entity,
    EventData,
    Feature,
    FeatureJobSettingAnalysis,
    FeatureStore,
    ItemData,
    PeriodicTask,
    Relationship,
    SlowlyChangingData,
    Workspace,
)
from featurebyte.api.feature import FeatureNamespace
from featurebyte.api.feature_list import FeatureList, FeatureListNamespace


class Catalog:
    """
    Catalog API object contains a bunch of helpers to easily access and view objects within Featurebyte.
    """

    @staticmethod
    @typechecked
    def list_features(
        include_id: Optional[bool] = False,
        entity: Optional[str] = None,
        data: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        List saved features

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        entity: Optional[str]
            Name of entity used to filter results
        data: Optional[str]
            Name of data used to filter results

        Returns
        -------
        pd.DataFrame
            Table of features
        """
        return Feature.list(include_id=include_id, entity=entity, data=data)

    @staticmethod
    @typechecked
    def list_feature_namespaces(
        include_id: Optional[bool] = False,
        entity: Optional[str] = None,
        data: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        List saved feature namespaces

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        entity: Optional[str]
            Name of entity used to filter results
        data: Optional[str]
            Name of data used to filter results

        Returns
        -------
        pd.DataFrame
            Table of feature namespaces
        """
        return FeatureNamespace.list(include_id=include_id, entity=entity, data=data)

    @staticmethod
    @typechecked
    def list_feature_list_namespaces(
        include_id: Optional[bool] = False,
        entity: Optional[str] = None,
        data: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        List saved feature list namespaces

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        entity: Optional[str]
            Name of entity used to filter results
        data: Optional[str]
            Name of data used to filter results

        Returns
        -------
        pd.DataFrame
            Table of feature list namespaces
        """
        return FeatureListNamespace.list(include_id=include_id, entity=entity, data=data)

    @staticmethod
    @typechecked
    def list_feature_lists(
        include_id: Optional[bool] = False,
        entity: Optional[str] = None,
        data: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        List saved feature lists

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        entity: Optional[str]
            Name of entity used to filter results
        data: Optional[str]
            Name of data used to filter results

        Returns
        -------
        pd.DataFrame
            Table of feature lists
        """
        return FeatureList.list(include_id=include_id, entity=entity, data=data)

    @staticmethod
    @typechecked
    def list_data(include_id: Optional[bool] = False, entity: Optional[str] = None) -> pd.DataFrame:
        """
        List saved data sources

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        entity: Optional[str]
            Name of entity used to filter results

        Returns
        -------
        DataFrame
            Table of data sources
        """
        return Data.list(include_id=include_id, entity=entity)

    @staticmethod
    @typechecked
    def list_dimension_data(
        include_id: Optional[bool] = False, entity: Optional[str] = None
    ) -> pd.DataFrame:
        """
        List saved dimension data sources

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        entity: Optional[str]
            Name of entity used to filter results

        Returns
        -------
        DataFrame
            Table of dimension data sources
        """
        return DimensionData.list(include_id=include_id, entity=entity)

    @staticmethod
    @typechecked
    def list_item_data(
        include_id: Optional[bool] = False, entity: Optional[str] = None
    ) -> pd.DataFrame:
        """
        List saved item data sources

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        entity: Optional[str]
            Name of entity used to filter results

        Returns
        -------
        DataFrame
            Table of item data sources
        """
        return ItemData.list(include_id=include_id, entity=entity)

    @staticmethod
    @typechecked
    def list_event_data(
        include_id: Optional[bool] = False, entity: Optional[str] = None
    ) -> pd.DataFrame:
        """
        List saved event data sources

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        entity: Optional[str]
            Name of entity used to filter results

        Returns
        -------
        DataFrame
            Table of event data sources
        """
        return EventData.list(include_id=include_id, entity=entity)

    @staticmethod
    @typechecked
    def list_scd_data(
        include_id: Optional[bool] = False, entity: Optional[str] = None
    ) -> pd.DataFrame:
        """
        List saved SCD data sources

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        entity: Optional[str]
            Name of entity used to filter results

        Returns
        -------
        DataFrame
            Table of SCD data sources
        """
        return SlowlyChangingData.list(include_id=include_id, entity=entity)

    @staticmethod
    @typechecked
    def list_relationships(
        include_id: Optional[bool] = True, relationship_type: Optional[Literal[tuple(RelationshipType)]] = None  # type: ignore[misc]
    ) -> pd.DataFrame:
        """
        List all relationships that exist in your FeatureByte instance, or filtered by relationship type.

        This provides a dataframe with:

        - the relationship id
        - primary entity
        - related entity
        - data source
        - enabled (whether the relationship is enabled)
        - creation timestamp
        - update timestamp
        - comments

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include the id in the dataframe
        relationship_type: Optional[Literal[tuple[RelationshipType]]]
            The type of relationship to list

        Returns
        -------
        pd.DataFrame
            A dataframe containing the relationships

        Examples
        --------
        List all relationships

        >>> import featurebyte as fb
        >>> fb.Relationship.list()  # doctest: +SKIP


        List all child-parent relationships

        >>> import featurebyte as fb
        >>> fb.Relationship.list(relationship_type="child_parent")  # doctest: +SKIP
        """
        return Relationship.list(include_id=include_id, relationship_type=relationship_type)

    @staticmethod
    @typechecked
    def list_feature_job_setting_analysis(
        include_id: Optional[bool] = False,
        event_data_id: Optional[ObjectId] = None,
    ) -> pd.DataFrame:
        """
        List saved feature job setting analysis

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        event_data_id: Optional[ObjectId]
            Event data id used to filter results

        Returns
        -------
        pd.DataFrame
            Table of feature job setting analysis
        """
        return FeatureJobSettingAnalysis.list(include_id=include_id, event_data_id=event_data_id)

    @staticmethod
    @typechecked
    def list_workspaces(include_id: Optional[bool] = False) -> pd.DataFrame:
        """
        List saved workspaces

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list

        Returns
        -------
        pd.DataFrame
            Table of workspaces
        """
        return Workspace.list(include_id=include_id)

    @staticmethod
    @typechecked
    def list_feature_stores(include_id: Optional[bool] = False) -> pd.DataFrame:
        """
        List saved feature stores

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list

        Returns
        -------
        pd.DataFrame
            Table of feature stores
        """
        return FeatureStore.list(include_id=include_id)

    @staticmethod
    @typechecked
    def list_entities(include_id: Optional[bool] = False) -> pd.DataFrame:
        """
        List saved entities

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list

        Returns
        -------
        pd.DataFrame
            Table of entities
        """
        return Entity.list(include_id=include_id)

    @staticmethod
    @typechecked
    def list_periodic_tasks(include_id: Optional[bool] = False) -> pd.DataFrame:
        """
        List saved periodic tasks

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list

        Returns
        -------
        pd.DataFrame
            Table of periodic tasks
        """
        return PeriodicTask.list(include_id=include_id)
