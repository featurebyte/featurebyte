"""
Catalog module
"""
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

import pandas as pd
from bson import ObjectId
from typeguard import typechecked

from featurebyte.api.api_object import SavableApiObject
from featurebyte.api.dimension_table import DimensionTable
from featurebyte.api.entity import Entity
from featurebyte.api.event_table import EventTable
from featurebyte.api.feature import Feature, FeatureNamespace
from featurebyte.api.feature_job_setting_analysis import FeatureJobSettingAnalysis
from featurebyte.api.feature_list import FeatureList, FeatureListNamespace
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.item_table import ItemTable
from featurebyte.api.periodic_task import PeriodicTask
from featurebyte.api.relationship import Relationship
from featurebyte.api.scd_table import SCDTable
from featurebyte.api.table import Table
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.config import activate_catalog, get_active_catalog_id
from featurebyte.models.catalog import CatalogModel
from featurebyte.models.relationship import RelationshipType
from featurebyte.schema.catalog import CatalogCreate, CatalogUpdate


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

    @staticmethod
    def list_features(
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
        return Feature.list_versions(
            include_id=include_id, feature_list_id=feature_list_id, entity=entity, data=data
        )

    @staticmethod
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
    def list_feature_lists(
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
        return FeatureList.list_versions(include_id=include_id)

    @staticmethod
    def list_tables(
        include_id: Optional[bool] = False, entity: Optional[str] = None
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
        return Table.list(include_id=include_id, entity=entity)

    @staticmethod
    def list_dimension_tables(
        include_id: Optional[bool] = False, entity: Optional[str] = None
    ) -> pd.DataFrame:
        """
        List saved dimension table sources

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        entity: Optional[str]
            Name of entity used to filter results

        Returns
        -------
        DataFrame
            Table of dimension table sources
        """
        return DimensionTable.list(include_id=include_id, entity=entity)

    @staticmethod
    def list_item_tables(
        include_id: Optional[bool] = False, entity: Optional[str] = None
    ) -> pd.DataFrame:
        """
        List saved item table sources

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        entity: Optional[str]
            Name of entity used to filter results

        Returns
        -------
        DataFrame
            Table of item table sources
        """
        return ItemTable.list(include_id=include_id, entity=entity)

    @staticmethod
    def list_event_tables(
        include_id: Optional[bool] = False, entity: Optional[str] = None
    ) -> pd.DataFrame:
        """
        List saved event table sources

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        entity: Optional[str]
            Name of entity used to filter results

        Returns
        -------
        DataFrame
            Table of event table sources
        """
        return EventTable.list(include_id=include_id, entity=entity)

    @staticmethod
    def list_scd_tables(
        include_id: Optional[bool] = False, entity: Optional[str] = None
    ) -> pd.DataFrame:
        """
        List saved SCD table sources

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        entity: Optional[str]
            Name of entity used to filter results

        Returns
        -------
        DataFrame
            Table of SCD table sources
        """
        return SCDTable.list(include_id=include_id, entity=entity)

    @staticmethod
    def list_relationships(
        include_id: Optional[bool] = True, relationship_type: Optional[Literal[tuple(RelationshipType)]] = None  # type: ignore
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
    def list_feature_job_setting_analyses(
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
        return FeatureJobSettingAnalysis.list(include_id=include_id, event_data_id=event_data_id)

    @staticmethod
    def list_catalogs(include_id: Optional[bool] = False) -> pd.DataFrame:
        """
        List saved catalogs

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list

        Returns
        -------
        pd.DataFrame
            Table of catalogs
        """
        return Catalog.list(include_id=include_id)

    @staticmethod
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
