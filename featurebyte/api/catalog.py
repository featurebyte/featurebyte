"""
Catalog module
"""
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Union

import pandas as pd
from bson import ObjectId
from typeguard import typechecked

from featurebyte.api.api_object import SavableApiObject
from featurebyte.api.api_object_util import NameAttributeUpdatableMixin
from featurebyte.api.catalog_decorator import update_and_reset_catalog
from featurebyte.api.catalog_get_by_id_mixin import CatalogGetByIdMixin
from featurebyte.api.data_source import DataSource
from featurebyte.api.entity import Entity
from featurebyte.api.feature import Feature
from featurebyte.api.feature_job_setting_analysis import FeatureJobSettingAnalysis
from featurebyte.api.feature_list import FeatureList
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.historical_feature_table import HistoricalFeatureTable
from featurebyte.api.observation_table import ObservationTable
from featurebyte.api.periodic_task import PeriodicTask
from featurebyte.api.relationship import Relationship
from featurebyte.api.table import Table
from featurebyte.api.view import View
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.config import activate_catalog, get_active_catalog_id
from featurebyte.exception import RecordRetrievalException
from featurebyte.logger import logger
from featurebyte.models.catalog import CatalogModel
from featurebyte.models.relationship import RelationshipType
from featurebyte.schema.catalog import CatalogCreate, CatalogUpdate


class Catalog(NameAttributeUpdatableMixin, SavableApiObject, CatalogGetByIdMixin):
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
    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.Catalog")

    # class variables
    _route = "/catalog"
    _update_schema_class = CatalogUpdate
    _list_schema = CatalogModel
    _get_schema = CatalogModel
    _list_fields = ["name", "created_at", "active"]

    def _get_create_payload(self) -> Dict[str, Any]:
        data = CatalogCreate(**self.json_dict())
        return data.json_dict()

    def info(  # pylint: disable=useless-parent-delegation
        self, verbose: bool = False
    ) -> Dict[str, Any]:
        """
        Returns a dictionary that summarizes the essential information of a Catalog object. The dictionary includes
        the following keys:

        - `name`: The name of the Catalog object.
        - `created_at`: The timestamp indicating when the Catalog object was created.
        - `updated_at`: The timestamp indicating when the Catalog object was last updated.

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
        Get info of a catalog object

        >>> catalog = fb.Catalog.get_or_create("grocery")  # doctest: +SKIP
        >>> catalog.info()  # doctest: +SKIP
        """
        return super().info(verbose)

    @classmethod
    def activate(cls, name: str) -> Catalog:
        """
        Activates and returns a Catalog object with the provided name. Exactly one catalog is active at any time.
        Only assets that belong to the active catalog are accessible.

        By employing a catalog, team members can effortlessly add, search, retrieve, and reuse the necessary tables,
        entities, features, and feature lists while obtaining comprehensive information about their properties. This
        information includes their type, creation date, related versions, readiness, status, and other descriptive
        details.

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
        logger.info(f"Catalog activated: {catalog.name}")
        return catalog

    @classmethod
    def create(
        cls,
        name: str,
    ) -> Catalog:
        """
        Creates a Catalog object that allows team members to easily add, search, retrieve, and reuse tables,
        entities, features, and feature lists. The Catalog provides detailed information about the properties of
        these elements, including their type, creation date, related versions, readiness, status, and other
        descriptive details.

        When dealing with data warehouses that cover multiple domains, creating several catalogs can aid in
        maintaining organization and simplifying management of data and features for different domains.

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
        Gets the currently active catalog.

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

    @typechecked
    def update_name(self, name: str) -> None:
        """
        Updates the catalog name.

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

    @classmethod
    def get(cls, name: str) -> Catalog:  # pylint: disable=useless-parent-delegation
        """
        Gets a Catalog object by its name.

        Parameters
        ----------
        name: str
            Name of the catalog to retrieve.

        Returns
        -------
        Catalog
            Catalog object.

        Examples
        --------
        Get a Catalog object that is already saved.

        >>> catalog = fb.Catalog.get("catalog_name")  # doctest: +SKIP
        """
        return super().get(name)

    @classmethod
    def get_by_id(  # pylint: disable=useless-parent-delegation
        cls, id: ObjectId  # pylint: disable=redefined-builtin,invalid-name
    ) -> Catalog:
        """
        Returns a Catalog object by its unique identifier (ID).

        Parameters
        ----------
        id: ObjectId
            Catalog unique identifier (ID).

        Returns
        -------
        Catalog
            Catalog object.

        Examples
        --------
        Get a Catalog object that is already saved.

        >>> fb.Catalog.get_by_id(<catalog_id>)  # doctest: +SKIP
        """
        return super().get_by_id(id=id)

    @classmethod
    def list(
        cls, include_id: Optional[bool] = True
    ) -> pd.DataFrame:  # pylint: disable=useless-parent-delegation
        """
        Returns a DataFrame containing information on catalogs such as their names, creation dates, and active status.

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
        List all Catalogs.

        >>> catalogs = fb.Catalog.list()
        """
        return super().list(include_id=include_id)

    @update_and_reset_catalog
    def create_entity(self, name: str, serving_names: List[str]) -> Entity:
        """
        Registers a new Entity object in the catalog.

        An Entity object defines an entity type represented in tables of the catalog, allowing for automatic table
        joins, unit of analysis, and organization of features, feature lists, and use cases.

        To create a new Entity object, you need to provide a name for the entity and its serving names.

        An entity's serving name is the name of the unique identifier that is used to identify the entity during a
        preview or serving request. Typically, the serving name for an entity is the name of the primary key (or
        natural key) of the table that represents the entity.

        For example, the serving names of a Customer entity could be 'CustomerID' and 'CustID'.

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
        include_id: Optional[bool] = True,
        primary_entity: Optional[Union[str, List[str]]] = None,
        primary_table: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """
        Generates a DataFrame that contains various attributes of the registered features, such as their names,
        types, corresponding tables, related entities, creation dates, and the state of readiness and online
        availability of their default version.

        The returned DataFrame can be filtered by the primary entity or the primary table of the features in the
        catalog.

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
        include_id: Optional[bool] = True,
        entity: Optional[str] = None,
        table: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Returns a DataFrame that contains various attributes of the registered feature lists. These attributes
        include the names of the feature lists, the number of features in each list, their status, whether they have
        been deployed in production, the percentage of production ready features of their default version, the tables
        used by the features, their related entities, and creation dates.

        The resulting DataFrame can be filtered based on the primary entity of the feature lists or the tables
        utilized by the feature lists.

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
        self, include_id: Optional[bool] = True, entity: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Returns a DataFrame that contains various attributes of the registered tables in the catalog, such as their
        names, types, statuses, creation dates, and associated entities.

        The returned DataFrame can be filtered by the name of the entities associated with the tables.

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
        include_id: Optional[bool] = True,
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
    def list_feature_stores(self, include_id: Optional[bool] = True) -> pd.DataFrame:
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
    def list_entities(self, include_id: Optional[bool] = True) -> pd.DataFrame:
        """
        Returns a DataFrame that contains various attributes of the entities registered in the catalog, such as
        their names, serving names and creation dates.

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
    def list_periodic_tasks(self, include_id: Optional[bool] = True) -> pd.DataFrame:
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
    def list_observation_tables(self, include_id: Optional[bool] = True) -> pd.DataFrame:
        """
        List saved observation tables.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.

        Returns
        -------
        pd.DataFrame
            Table of observation tables.

        Examples
        --------
        List saved observation tables.

        >>> observation_tables = catalog.list_observation_tables()
        """
        return ObservationTable.list(include_id=include_id)

    @update_and_reset_catalog
    def list_historical_feature_tables(self, include_id: Optional[bool] = True) -> pd.DataFrame:
        """
        List saved historical feature tables.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.

        Returns
        -------
        pd.DataFrame
            Table of historical feature tables.

        Examples
        --------
        List saved historical feature tables.

        >>> historical_feature_tables = catalog.list_historical_feature_tables()
        """
        return HistoricalFeatureTable.list(include_id=include_id)

    @update_and_reset_catalog
    def get_data_source(self, feature_store_name: str) -> DataSource:
        """
        Gets a data source based on the name of the feature store that the data source is associated with.

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
        return feature_store.get_data_source()  # pylint: disable=no-member

    @update_and_reset_catalog
    def get_view(self, table_name: str) -> View:
        """
        Gets a View object from the catalog based on the name of the table the view should be derived from.

        You have the option to choose between two view construction modes: auto and manual, with auto being the
        default mode.

        When using the auto mode, the data accessed through the view is cleaned based on the default cleaning
        operations specified in the catalog table and special columns such as the record creation timestamp that
        are not intended for feature engineering are not included in the view columns.

        In manual mode, the default cleaning operations are not applied, and you have the flexibility to define your
        own cleaning operations.

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
        Gets a Feature object from the catalog using the feature's name and optionally its version name. If no
        version name is provided, the default version of the feature is returned.

        The Feature object includes the logical plan for computing the feature and can be used to create a FeatureList
        object for training and predicting machine learning models for a specific use case.

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
        Gets a FeatureList object from the catalog by specifying the feature list's name and, optionally,
        its version name. If the version name is not provided, the default version of the feature list will
        be returned.

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
        Gets a Table object from the catalog based on its name.

        A Table object represents a source table within the data warehouse and provides metadata to support feature
        engineering. There are four distinct types of Table objects: EventTable, ItemTable, DimensionTable,
        and SCDTable.

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
        Gets a Relationship object by name.

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
        Gets an Entity object from the catalog based on its name.

        An Entity object defines an entity type represented in tables of the catalog, allowing for automatic table
        joins, unit of analysis, and organization of features, feature lists, and use cases.

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

    @update_and_reset_catalog
    def get_observation_table(self, name: str) -> ObservationTable:
        """
        Get observation table by name.

        Parameters
        ----------
        name: str
            Observation table name.

        Returns
        -------
        ObservationTable
            Observation table object.

        Examples
        --------
        Get a saved observation table.

        >>> observation_table = catalog.get_observation_table("observation_table_name")  # doctest: +SKIP
        """
        return ObservationTable.get(name=name)

    @update_and_reset_catalog
    def get_historical_feature_table(self, name: str) -> HistoricalFeatureTable:
        """
        Get historical feature table by name.

        Parameters
        ----------
        name: str
            Historical feature table name.

        Returns
        -------
        HistoricalFeatureTable
            Historical feature table object.

        Examples
        --------
        Get a saved historical feature table.

        >>> historical_feature_table = catalog.get_historical_feature_table("historical_feature_table_name")  # doctest: +SKIP
        """
        return HistoricalFeatureTable.get(name=name)
