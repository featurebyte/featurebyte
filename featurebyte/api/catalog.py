"""
Catalog module
"""
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Union

from functools import wraps

import pandas as pd
from bson import ObjectId
from pydantic import Field
from typeguard import typechecked

from featurebyte.api.api_object import SavableApiObject
from featurebyte.api.api_object_util import NameAttributeUpdatableMixin
from featurebyte.api.data_source import DataSource
from featurebyte.api.entity import Entity
from featurebyte.api.feature import Feature
from featurebyte.api.feature_job_setting_analysis import FeatureJobSettingAnalysis
from featurebyte.api.feature_list import FeatureList
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.periodic_task import PeriodicTask
from featurebyte.api.relationship import Relationship
from featurebyte.api.table import Table
from featurebyte.api.view import View
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.config import activate_catalog, get_active_catalog_id
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.catalog import CatalogModel
from featurebyte.models.relationship import RelationshipType
from featurebyte.schema.catalog import CatalogCreate, CatalogUpdate


def update_and_reset_catalog(func: Any) -> Any:
    """
    Decorator to update the catalog and reset it back to original state if needed.

    If the calling catalog object has the same ID as the global state, we will just call the function that is being
    decorated.
    If not, this decorator will temporarily update the global catalog state to the catalog_id of the calling catalog
    object, call the decorated function, and then reset the state back.

    This is useful as an intermediate state for us to support a catalog object oriented syntax, while still maintaining
    a global state for the catalog ID at the implementation level.

    Parameters
    ----------
    func: Any
        Function to decorate

    Returns
    -------
    Any
    """

    @wraps(func)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        active_catalog_id = get_active_catalog_id()
        # If the catalog is already active, just call the function
        if self.id == active_catalog_id:
            return func(self, *args, **kwargs)
        # Activate catalog of object
        activate_catalog(self.id)
        try:
            return func(self, *args, **kwargs)
        finally:
            # Reset catalog back to original state
            activate_catalog(active_catalog_id)

    return wrapper


@typechecked
class Catalog(NameAttributeUpdatableMixin, SavableApiObject):
    """
    A FeatureByte Catalog serves as a centralized repository for storing metadata about FeatureByte objects such as
    tables, entities, features, and feature lists associated with a specific domain. It functions as an effective tool
    for facilitating collaboration among team members working on similar use cases or utilizing the same data source
    within a data warehouse.

    By employing a catalog, team members can effortlessly search, retrieve, and reuse the necessary tables,
    entities, features, and feature lists while obtaining comprehensive information about their properties.
    This information includes their type, creation date, related versions, status, and other descriptive details.

    For data warehouses covering multiple domains, creating multiple catalogs can help maintain organization and
    simplify management of the data and features.
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

    # pydantic instance variable (public)
    saved: bool = Field(
        default=False,
        allow_mutation=False,
        exclude=True,
        description="Flag to indicate whether the Catalog object is saved in the FeatureByte catalog.",
    )

    def _get_create_payload(self) -> Dict[str, Any]:
        data = CatalogCreate(**self.json_dict())
        return data.json_dict()

    @classmethod
    def activate(cls, name: str) -> Catalog:
        """
        Activate catalog with the provided name. Exactly one catalog is active at any time.
        If no catalog has been activated, the default catalog will be active.
        Only assets that belong to the active catalog are accessible.

        Parameters
        ----------
        name: str
            Name of catalog to activate.

        Returns
        -------
        Catalog

        Examples
        --------
        >>> catalog = Catalog.activate("default")
        >>> catalog.list_tables()[["name", "type"]]
        Empty DataFrame
        Columns: [name, type]
        Index: []
        >>> catalog = Catalog.activate("grocery")
        >>> catalog.list_tables()[["name", "type"]]
                        name             type
        0     GROCERYPRODUCT  dimension_table
        1    GROCERYCUSTOMER        scd_table
        2       INVOICEITEMS       item_table
        3     GROCERYINVOICE      event_table
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
        Create and return an instance of a catalog. The catalog will be activated.

        Parameters
        ----------
        name: str
            Name of catalog to create.

        Returns
        -------
        Catalog

        Examples
        --------
        Create a new catalog.

        >>> catalog = fb.Catalog.create("new_catalog")  # doctest: +SKIP

        See Also
        --------
        - [Catalog.get_or_create](/reference/featurebyte.api.catalog.Catalog.get_or_create/): Get or create Catalog
        """
        catalog = cls(name=name)
        catalog.save()
        activate_catalog(catalog.id)
        return catalog

    @classmethod
    def get_or_create(
        cls,
        name: str,
    ) -> Catalog:
        """
        Create and return an instance of a catalog. If a catalog with the same name already exists,
        return that instead. The catalog will be activated.

        Parameters
        ----------
        name: str
            Name of catalog to get or create.

        Returns
        -------
        Catalog

        Examples
        --------
        Create a new catalog

        >>> catalog = fb.Catalog.get_or_create("grocery")
        >>> fb.Catalog.list()[["name", "active"]]
              name  active
        0  grocery    True
        1  default   False

        See Also
        --------
        - [Catalog.create](/reference/featurebyte.api.catalog.Catalog.create/): Create Catalog
        """
        try:
            catalog = Catalog.get(name=name)
            activate_catalog(catalog.id)
            return catalog
        except RecordRetrievalException:
            return Catalog.create(name=name)

    @classmethod
    def get_active(cls) -> Catalog:
        """
        Get the currently active catalog.

        Returns
        -------
        Catalog
            The currently active catalog.

        Examples
        --------
        >>> catalog = fb.Catalog.get_active()
        >>> catalog.name
        'grocery'
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
        Update catalog name.

        Parameters
        ----------
        name: str
            New catalog name.

        Examples
        --------
        >>> catalog = fb.Catalog.get_active()
        >>> catalog.update_name("grocery_store")
        >>> catalog.name
        'grocery_store'
        >>> catalog.update_name("grocery")
        >>> catalog.name
        'grocery'
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

    @update_and_reset_catalog
    def create_entity(self, name: str, serving_names: List[str]) -> Entity:
        """
        Create a new entity with the provided name and serving names.

        Parameters
        ----------
        name: str
            Entity name
        serving_names: List[str]
            Names of the serving columns

        Returns
        -------
        Entity
            Newly created entity.

        Examples
        --------
        Create a new entity.

        >>> entity = catalog.create_entity("customer", ["customer_id"])
        """
        return Entity.create(name=name, serving_names=serving_names)

    @update_and_reset_catalog
    def list_features(
        self,
        include_id: Optional[bool] = False,
        primary_entity: Optional[Union[str, List[str]]] = None,
        primary_table: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """
        List features that have been saved.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        primary_entity: Optional[Union[str, List[str]]]
            Name of entity used to filter results. If multiple entities are provided, the filtered results will
            contain features that are associated with all the entities.
        primary_table: Optional[Union[str, List[str]]]
            Name of table used to filter results. If multiple tables are provided, the filtered results will
            contain features that are associated with all the tables.

        Returns
        -------
        pd.DataFrame
            Table of features

        Examples
        --------
        List saved features.

        >>> features = catalog.list_features()
        """
        return Feature.list(
            include_id=include_id, primary_entity=primary_entity, primary_table=primary_table
        )

    @update_and_reset_catalog
    def list_feature_lists(
        self,
        include_id: Optional[bool] = False,
        entity: Optional[str] = None,
        table: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        List saved feature lists.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.
        entity: Optional[str]
            Name of entity used to filter results.
        table: Optional[str]
            Name of table used to filter results.

        Returns
        -------
        pd.DataFrame
            Table of feature lists.

        Examples
        --------
        List saved feature lists.

        >>> feature_lists = catalog.list_feature_lists()
        """
        return FeatureList.list(include_id=include_id, entity=entity, table=table)

    @update_and_reset_catalog
    def list_tables(
        self, include_id: Optional[bool] = False, entity: Optional[str] = None
    ) -> pd.DataFrame:
        """
        List saved tables.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.
        entity: Optional[str]
            Name of entity used to filter results.

        Returns
        -------
        pd.DataFrame
            Dataframe of tables

        Examples
        --------
        List saved tables.

        >>> tables = catalog.list_tables()
        """
        return Table.list(include_id=include_id, entity=entity)

    @update_and_reset_catalog
    def list_relationships(
        self, include_id: Optional[bool] = True, relationship_type: Optional[Literal[tuple(RelationshipType)]] = None  # type: ignore
    ) -> pd.DataFrame:
        """
        List all relationships that exist in your FeatureByte instance, or filtered by relationship type.

        This provides a dataframe with:

        - the relationship id
        - primary entity
        - related entity
        - table source
        - enabled (whether the relationship is enabled)
        - creation timestamp
        - update timestamp
        - comments

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include the id in the dataframe.
        relationship_type: Optional[Literal[tuple[RelationshipType]]]
            The type of relationship to list.

        Returns
        -------
        pd.DataFrame
            A dataframe containing the relationships.

        Examples
        --------
        List all relationships.

        >>> catalog.list_relationships()[[
        ...     "relationship_type",
        ...     "primary_entity",
        ...     "related_entity",
        ... ]]
          relationship_type   primary_entity   related_entity
        0      child_parent   groceryinvoice  grocerycustomer
        1      child_parent  grocerycustomer      frenchstate

        List all child-parent relationships.

        >>> catalog.list_relationships(relationship_type="child_parent")[[
        ...     "relationship_type",
        ...     "primary_entity",
        ...     "related_entity",
        ... ]]
          relationship_type   primary_entity   related_entity
        0      child_parent   groceryinvoice  grocerycustomer
        1      child_parent  grocerycustomer      frenchstate
        """
        return Relationship.list(include_id=include_id, relationship_type=relationship_type)

    @update_and_reset_catalog
    def list_feature_job_setting_analyses(
        self,
        include_id: Optional[bool] = False,
        event_table_id: Optional[ObjectId] = None,
    ) -> pd.DataFrame:
        """
        List saved feature job setting analyses.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.
        event_table_id: Optional[ObjectId]
            EventTable id used to filter results.

        Returns
        -------
        pd.DataFrame
            Table of feature job setting analysis.

        Examples
        --------
        List saved feature job setting analyses.

        >>> feature_job_setting_analyses = catalog.list_feature_job_setting_analyses()
        """
        return FeatureJobSettingAnalysis.list(include_id=include_id, event_table_id=event_table_id)

    @update_and_reset_catalog
    def list_feature_stores(self, include_id: Optional[bool] = False) -> pd.DataFrame:
        """
        List saved feature stores.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.

        Returns
        -------
        pd.DataFrame
            Table of feature stores

        Examples
        --------
        List saved feature stores.

        >>> feature_stores = catalog.list_feature_stores()
        """
        return FeatureStore.list(include_id=include_id)

    @update_and_reset_catalog
    def list_entities(self, include_id: Optional[bool] = False) -> pd.DataFrame:
        """
        List saved entities.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.

        Returns
        -------
        pd.DataFrame
            Table of entities.

        Examples
        --------
        List saved entities.

        >>> entities = catalog.list_entities()
        """
        return Entity.list(include_id=include_id)

    @update_and_reset_catalog
    def list_periodic_tasks(self, include_id: Optional[bool] = False) -> pd.DataFrame:
        """
        List saved periodic tasks.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.

        Returns
        -------
        pd.DataFrame
            Table of periodic tasks.

        Examples
        --------
        List saved periodic tasks.

        >>> periodic_tasks = catalog.list_periodic_tasks()
        """
        return PeriodicTask.list(include_id=include_id)

    @update_and_reset_catalog
    def get_data_source(self, feature_store_name: str) -> DataSource:
        """
        Get data source by a given feature store name.

        Parameters
        ----------
        feature_store_name: str
            Feature store name.

        Returns
        -------
        DataSource
            Data source object

        Examples
        --------
        Get data source.

        >>> data_source = catalog.get_data_source("playground")
        """
        feature_store = FeatureStore.get(name=feature_store_name)
        return feature_store.get_data_source()

    @update_and_reset_catalog
    def get_view(self, table_name: str) -> View:
        """
        Get view for a given table name.

        Parameters
        ----------
        table_name: str
            Table name.

        Returns
        -------
        View
            View object.

        Examples
        --------
        Get an event view from an event table.

        >>> event_view = catalog.get_view("GROCERYINVOICE")
        """
        table = Table.get(name=table_name)
        return table.get_view()  # type: ignore[no-any-return]

    @update_and_reset_catalog
    def get_feature(self, name: str, version: Optional[str] = None) -> Feature:
        """
        Get a feature by name and version.

        Parameters
        ----------
        name: str
            Feature name.
        version: Optional[str]
            Feature version. If None is provided, the default version will be returned.

        Returns
        -------
        Feature
            Feature object.

        Examples
        --------
        Get a saved feature.

        >>> feature = catalog.get_feature("InvoiceAmountAvg_60days")
        """
        return Feature.get(name=name, version=version)

    @update_and_reset_catalog
    def get_feature_list(self, name: str, version: Optional[str] = None) -> FeatureList:
        """
        Get feature list by name.

        Parameters
        ----------
        name: str
            Feature list name.
        version: Optional[str]
            Version of the feature list. If None, the default version will be returned.

        Returns
        -------
        FeatureList
            Feature list object.

        Examples
        --------
        Get a saved feature list.

        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        """
        return FeatureList.get(name=name, version=version)

    @update_and_reset_catalog
    def get_table(self, name: str) -> Any:
        """
        Get table by name.

        Parameters
        ----------
        name: str
            Table name.

        Returns
        -------
        Any
            Retrieved table.

        Examples
        --------
        Get a saved table.

        >>> item_table = catalog.get_table("INVOICEITEMS")
        """
        return Table.get(name=name)

    @update_and_reset_catalog
    def get_relationship(self, name: str) -> Relationship:
        """
        Get relationship by name.

        Parameters
        ----------
        name: str
            Relationship name.

        Returns
        -------
        Relationship
            Relationship object.

        Examples
        --------
        Get a saved relationship.

        >>> relationship = catalog.get_relationship("relationship_name")  # doctest: +SKIP
        """
        return Relationship.get(name=name)

    @update_and_reset_catalog
    def get_feature_job_setting_analysis(self, name: str) -> FeatureJobSettingAnalysis:
        """
        Get feature job setting analysis by name.

        Parameters
        ----------
        name: str
            Feature job setting analysis name.

        Returns
        -------
        FeatureJobSettingAnalysis
            Feature job setting analysis object.

        Examples
        --------
        Get a saved feature job setting analysis.

        >>> feature_job_setting_analysis = catalog.get_feature_job_setting_analysis("analysis_name")  # doctest: +SKIP
        """
        return FeatureJobSettingAnalysis.get(name=name)

    @update_and_reset_catalog
    def get_feature_store(self, name: str) -> FeatureStore:
        """
        Get feature store by name.

        Parameters
        ----------
        name: str
            Feature store name.

        Returns
        -------
        FeatureStore
            Feature store object.

        Examples
        --------
        Get a saved feature store.

        >>> feature_store = catalog.get_feature_store("playground")
        """
        return FeatureStore.get(name=name)

    @update_and_reset_catalog
    def get_entity(self, name: str) -> Entity:
        """
        Get entity by name.

        Parameters
        ----------
        name: str
            Entity name.

        Returns
        -------
        Entity
            Entity object.

        Examples
        --------
        Get a saved entity.

        >>> entity = catalog.get_entity("grocerycustomer")
        """
        return Entity.get(name=name)

    @update_and_reset_catalog
    def get_periodic_task(self, name: str) -> PeriodicTask:
        """
        Get periodic task by name.

        Parameters
        ----------
        name: str
            Periodic task name.

        Returns
        -------
        PeriodicTask
            Periodic task object.

        Examples
        --------
        Get a saved periodic task.

        >>> periodic_task = catalog.get_periodic_task("task_name")  # doctest: +SKIP
        """
        return PeriodicTask.get(name=name)
