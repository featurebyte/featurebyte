"""
Catalog module
"""
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

import pandas as pd
from bson import ObjectId
from typeguard import typechecked

from featurebyte.api.api_object import SavableApiObject
from featurebyte.api.entity import Entity
from featurebyte.api.feature import Feature, FeatureNamespace
from featurebyte.api.feature_job_setting_analysis import FeatureJobSettingAnalysis
from featurebyte.api.feature_list import FeatureList, FeatureListNamespace
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.periodic_task import PeriodicTask
from featurebyte.api.relationship import Relationship
from featurebyte.api.table import Table
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.config import activate_catalog, get_active_catalog_id
from featurebyte.models.catalog import CatalogModel
from featurebyte.models.relationship import RelationshipType
from featurebyte.schema.catalog import CatalogCreate, CatalogUpdate


def check_is_active_catalog(catalog_id: ObjectId) -> None:
    """
    Decorator to check if the catalog is active before calling the function.

    Parameters
    ----------
    catalog_id: ObjectId
        Catalog ID

    Raises
    ------
    ValueError
        If catalog is not active
    """
    if catalog_id != get_active_catalog_id():
        raise ValueError("Catalog is not active. Please activate the catalog first.")


@typechecked
class Catalog(CatalogModel, SavableApiObject):
    """
    Catalog API object contains a bunch of helpers to easily access and view objects within Featurebyte.
    """

    # pylint: disable=too-many-public-methods

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Catalog"], proxy_class="featurebyte.Catalog")

    # class variables
    _route = "/catalog"
    _update_schema_class = CatalogUpdate
    _list_schema = CatalogModel
    _get_schema = CatalogModel
    _list_fields = ["name", "created_at", "active"]

    def _get_create_payload(self) -> Dict[str, Any]:
        data = CatalogCreate(**self.json_dict())
        return data.json_dict()

    @classmethod
    def activate(cls, name: str) -> Catalog:
        """
        Activate catalog by name

        Parameters
        ----------
        name: str
            Name of catalog to activate

        Returns
        -------
        Catalog
        """
        catalog = cls.get(name)
        activate_catalog(catalog.id)
        return catalog

    @classmethod
    def create(
        cls,
        name: str,
    ) -> Catalog:
        """
        Create and activate catalog

        Parameters
        ----------
        name: str
            feature store name

        Returns
        -------
        Catalog

        Examples
        --------
        Create a new catalog

        >>> from featurebyte import Catalog
        >>> Catalog.create(  # doctest: +SKIP
        ...     name="My Catalog"
        ... )

        List catalogs
        >>> Catalog.list()  # doctest: +SKIP
                                  id	             name	             created_at	active
        0	63ef2ca50523266031b728dd	     My Catalog	2023-02-17 07:28:37.368   True
        1	63eda344d0313fb925f7883a	          default	2023-02-17 07:03:26.267	 False
        """
        catalog = Catalog(name=name)
        catalog.save()
        activate_catalog(catalog.id)
        return catalog

    @classmethod
    def get_active(cls) -> Catalog:
        """
        Get active catalog

        Returns
        -------
        Catalog
        """
        return cls.get_by_id(get_active_catalog_id())

    @classmethod
    def _post_process_list(cls, item_list: pd.DataFrame) -> pd.DataFrame:
        item_list = super()._post_process_list(item_list)

        # add column to indicate whether catalog is active
        item_list["active"] = item_list.id == get_active_catalog_id()

        return item_list

    def update_name(self, name: str) -> None:
        """
        Change entity name

        Parameters
        ----------
        name: str
            New entity name
        """
        self.update(update_payload={"name": name}, allow_update_local=True)

    @property
    def name_history(self) -> List[Dict[str, Any]]:
        """
        List of name history entries

        Returns
        -------
        list[dict[str, Any]]
        """
        return self._get_audit_history(field_name="name")

    def list_features(
        self,
        include_id: Optional[bool] = False,
        feature_list_id: Optional[ObjectId] = None,
        entity: Optional[str] = None,
        data: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        List saved feature versions

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        feature_list_id: Optional[ObjectId] = None,
            Include only features in specified feature list
        entity: Optional[str]
            Name of entity used to filter results
        data: Optional[str]
            Name of data used to filter results

        Returns
        -------
        pd.DataFrame
            Table of features
        """
        check_is_active_catalog(self.id)
        return Feature.list_versions(
            include_id=include_id, feature_list_id=feature_list_id, entity=entity, data=data
        )

    def list_feature_namespaces(
        self,
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
        check_is_active_catalog(self.id)
        return FeatureNamespace.list(include_id=include_id, entity=entity, data=data)

    def list_feature_list_namespaces(
        self,
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
        check_is_active_catalog(self.id)
        return FeatureListNamespace.list(include_id=include_id, entity=entity, data=data)

    def list_feature_lists(
        self,
        include_id: Optional[bool] = False,
    ) -> pd.DataFrame:
        """
        List saved feature lists

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list

        Returns
        -------
        pd.DataFrame
            Table of feature lists
        """
        check_is_active_catalog(self.id)
        return FeatureList.list_versions(include_id=include_id)

    def list_tables(
        self, include_id: Optional[bool] = False, entity: Optional[str] = None
    ) -> pd.DataFrame:
        """
        List saved tables

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        entity: Optional[str]
            Name of entity used to filter results

        Returns
        -------
        pd.DataFrame
            Dataframe of tables
        """
        check_is_active_catalog(self.id)
        return Table.list(include_id=include_id, entity=entity)

    def list_relationships(
        self, include_id: Optional[bool] = True, relationship_type: Optional[Literal[tuple(RelationshipType)]] = None  # type: ignore
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
        check_is_active_catalog(self.id)
        return Relationship.list(include_id=include_id, relationship_type=relationship_type)

    def list_feature_job_setting_analyses(
        self,
        include_id: Optional[bool] = False,
        event_data_id: Optional[ObjectId] = None,
    ) -> pd.DataFrame:
        """
        List saved feature job setting analyses

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
        check_is_active_catalog(self.id)
        return FeatureJobSettingAnalysis.list(include_id=include_id, event_data_id=event_data_id)

    def list_feature_stores(self, include_id: Optional[bool] = False) -> pd.DataFrame:
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
        check_is_active_catalog(self.id)
        return FeatureStore.list(include_id=include_id)

    def list_entities(self, include_id: Optional[bool] = False) -> pd.DataFrame:
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
        check_is_active_catalog(self.id)
        return Entity.list(include_id=include_id)

    def list_periodic_tasks(self, include_id: Optional[bool] = False) -> pd.DataFrame:
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
        check_is_active_catalog(self.id)
        return PeriodicTask.list(include_id=include_id)

    def get_feature(self, name: str, version: Optional[str] = None) -> Feature:
        """
        Get a feature by name and version

        Parameters
        ----------
        name: str
            Feature name
        version: Optional[str]
            Feature version, if None, the default version will be returned

        Returns
        -------
        Feature
            Feature object
        """
        check_is_active_catalog(self.id)
        return Feature.get(name=name, version=version)

    def get_feature_namespace(self, name: str) -> FeatureNamespace:
        """
        Get a feature namespace by name

        Parameters
        ----------
        name: str
            Feature namespace name

        Returns
        -------
        FeatureNamespace
            Feature namespace object
        """
        check_is_active_catalog(self.id)
        return FeatureNamespace.get(name=name)

    def get_feature_list_namespace(self, name: str) -> FeatureListNamespace:
        """
        Get a feature list namespace by name

        Parameters
        ----------
        name: str
            Feature list namespace name

        Returns
        -------
        FeatureListNamespace
            Feature list namespace object
        """
        check_is_active_catalog(self.id)
        return FeatureListNamespace.get(name=name)

    def get_feature_list(self, name: str, version: Optional[str] = None) -> FeatureList:
        """
        Get feature list by name

        Parameters
        ----------
        name: str
            Feature list name
        version: Optional[str]
            Version of the feature list, if None, the default version will be returned

        Returns
        -------
        FeatureList
            Feature list object
        """
        check_is_active_catalog(self.id)
        return FeatureList.get(name=name, version=version)

    def get_table(self, name: str) -> Any:
        """
        Get table by name

        Parameters
        ----------
        name: str
            Table name

        Returns
        -------
        Any
            Retrieved source table
        """
        check_is_active_catalog(self.id)
        return Table.get(name=name)

    def get_relationship(self, name: str) -> Relationship:
        """
        Get relationship by name

        Parameters
        ----------
        name: str
            Relationship name

        Returns
        -------
        Relationship
            Relationship object
        """
        check_is_active_catalog(self.id)
        return Relationship.get(name=name)

    def get_feature_job_setting_analysis(self, name: str) -> FeatureJobSettingAnalysis:
        """
        Get feature job setting analysis by name

        Parameters
        ----------
        name: str
            Feature job setting analysis name

        Returns
        -------
        FeatureJobSettingAnalysis
            Feature job setting analysis object
        """
        check_is_active_catalog(self.id)
        return FeatureJobSettingAnalysis.get(name=name)

    def get_feature_store(self, name: str) -> FeatureStore:
        """
        Get feature store by name

        Parameters
        ----------
        name: str
            Feature store name

        Returns
        -------
        FeatureStore
            Feature store object
        """
        check_is_active_catalog(self.id)
        return FeatureStore.get(name=name)

    def get_entity(self, name: str) -> Entity:
        """
        Get entity by name

        Parameters
        ----------
        name: str
            Entity name

        Returns
        -------
        Entity
            Entity object
        """
        check_is_active_catalog(self.id)
        return Entity.get(name=name)

    def get_periodic_task(self, name: str) -> PeriodicTask:
        """
        Get periodic task by name

        Parameters
        ----------
        name: str
            Periodic task name

        Returns
        -------
        PeriodicTask
            Periodic task object
        """
        check_is_active_catalog(self.id)
        return PeriodicTask.get(name=name)
