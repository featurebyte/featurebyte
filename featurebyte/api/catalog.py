"""
Catalog module
"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Optional, Union

import pandas as pd
from bson import ObjectId
from pydantic import Field
from typeguard import typechecked

from featurebyte.api.api_handler.base import ListHandler
from featurebyte.api.api_handler.catalog import CatalogListHandler
from featurebyte.api.api_object_util import NameAttributeUpdatableMixin
from featurebyte.api.batch_feature_table import BatchFeatureTable
from featurebyte.api.batch_request_table import BatchRequestTable
from featurebyte.api.catalog_decorator import update_and_reset_catalog
from featurebyte.api.catalog_get_by_id_mixin import CatalogGetByIdMixin
from featurebyte.api.context import Context
from featurebyte.api.data_source import DataSource
from featurebyte.api.deployment import Deployment
from featurebyte.api.entity import Entity
from featurebyte.api.feature import Feature
from featurebyte.api.feature_job_setting_analysis import FeatureJobSettingAnalysis
from featurebyte.api.feature_list import FeatureList
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.historical_feature_table import HistoricalFeatureTable
from featurebyte.api.observation_table import ObservationTable
from featurebyte.api.online_store import OnlineStore
from featurebyte.api.periodic_task import PeriodicTask
from featurebyte.api.relationship import Relationship
from featurebyte.api.savable_api_object import SavableApiObject
from featurebyte.api.static_source_table import StaticSourceTable
from featurebyte.api.table import Table
from featurebyte.api.target import Target
from featurebyte.api.use_case import UseCase
from featurebyte.api.user_defined_function import UserDefinedFunction
from featurebyte.api.view import View
from featurebyte.common import activate_catalog, get_active_catalog_id
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.exception import RecordRetrievalException
from featurebyte.logging import get_logger
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.catalog import CatalogModel
from featurebyte.models.relationship import RelationshipType
from featurebyte.schema.catalog import CatalogCreate, CatalogOnlineStoreUpdate, CatalogUpdate

logger = get_logger(__name__)


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

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.Catalog")
    _route: ClassVar[str] = "/catalog"
    _update_schema_class: ClassVar[Any] = CatalogUpdate
    _list_schema: ClassVar[Any] = CatalogModel
    _get_schema: ClassVar[Any] = CatalogModel
    _list_fields: ClassVar[List[str]] = ["name", "created_at", "active"]

    # pydantic instance variable (internal use)
    internal_default_feature_store_ids: List[PydanticObjectId] = Field(
        alias="default_feature_store_ids"
    )
    internal_online_store_id: Optional[PydanticObjectId] = Field(
        default=None, alias="online_store_id"
    )

    @property
    def default_feature_store_ids(self) -> List[PydanticObjectId]:
        """
        Feature store IDs associated with the catalog. This should only have one ID, until we support multiple
        feature stores at a later point.

        Returns
        -------
        List[PydanticObjectId]
            Feature store IDs
        """
        try:
            return self.cached_model.default_feature_store_ids
        except RecordRetrievalException:
            return self.internal_default_feature_store_ids

    @property
    def online_store_id(self) -> Optional[PydanticObjectId]:
        """
        Returns the online store ID associated with the catalog.

        Returns
        -------
        Optional[PydanticObjectId]
        """
        try:
            return self.cached_model.online_store_id
        except RecordRetrievalException:
            return self.internal_online_store_id

    @property
    def online_store(self) -> OnlineStore:
        """
        Returns the online store associated with the catalog.

        Returns
        -------
        OnlineStore

        Raises
        ------
        ValueError
            If the catalog does not have an associated online store.
        """
        if not self.online_store_id:
            raise ValueError("Catalog does not have an associated online store.")
        return OnlineStore.get_by_id(self.online_store_id)

    def _get_create_payload(self) -> Dict[str, Any]:
        data = CatalogCreate(**self.model_dump(by_alias=True))
        return data.json_dict()

    def info(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Returns a dictionary that summarizes the essential information of a Catalog object. The dictionary includes
        the following keys:

        - `name`: The name of the Catalog object.
        - `created_at`: The timestamp indicating when the Catalog object was created.
        - `updated_at`: The timestamp indicating when the Catalog object was last updated.

        Parameters
        ----------
        verbose: bool
            The parameter "verbose" in the current state of the code does not have any impact on the output.

        Returns
        -------
        Dict[str, Any]
            Key-value mapping of properties of the object.

        Examples
        --------
        Get info of a catalog object

        >>> catalog = fb.Catalog.get_or_create("grocery", "playground")  # doctest: +SKIP
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

        Raises
        ------
        RecordRetrievalException
            If the catalog with the provided name does not exist.

        Examples
        --------
        >>> catalog = Catalog.activate("grocery")
        >>> catalog.list_tables()[["name", "type"]]
                        name             type
        0     GROCERYPRODUCT  dimension_table
        1    GROCERYCUSTOMER        scd_table
        2       INVOICEITEMS       item_table
        3     GROCERYINVOICE      event_table
        """
        current_catalog_id = get_active_catalog_id()
        activate_catalog(None)
        try:
            catalog = cls.get(name)
        except RecordRetrievalException:
            activate_catalog(current_catalog_id)
            raise
        activate_catalog(catalog.id)
        logger.info(f"Catalog activated: {catalog.name}")
        return catalog

    @classmethod
    def create(
        cls,
        name: str,
        feature_store_name: str,
        online_store_name: Optional[str] = None,
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
        feature_store_name: str
            Name of feature store to associate with the catalog that we are creating.
        online_store_name: Optional[str]
            Name of online store to associate with the catalog that we are creating.

        Returns
        -------
        Catalog

        Examples
        --------
        Create a new catalog.

        >>> catalog = fb.Catalog.create(
        ...     "new_catalog", "feature_store_name", "mysql_online_store"
        ... )  # doctest: +SKIP
        """
        feature_store = FeatureStore.get(feature_store_name)
        if online_store_name:
            online_store_id = OnlineStore.get(online_store_name).id
        else:
            online_store_id = None
        catalog = cls(
            name=name, default_feature_store_ids=[feature_store.id], online_store_id=online_store_id
        )
        catalog.save()
        activate_catalog(catalog.id)
        return catalog

    @classmethod
    def get_or_create(
        cls,
        name: str,
        feature_store_name: str,
        online_store_name: Optional[str] = None,
    ) -> Catalog:
        """
        Create and return an instance of a catalog. If a catalog with the same name already exists,
        return that instead. The catalog will be activated.

        Parameters
        ----------
        name: str
            Name of catalog to get or create.
        feature_store_name: str
            Name of feature store to associate with the catalog that we are creating.
        online_store_name: Optional[str]
            Name of online store to associate with the catalog that we are creating.

        Returns
        -------
        Catalog

        Examples
        --------
        Create a new catalog

        >>> catalog = fb.Catalog.get_or_create("grocery", "playground")
        >>> fb.Catalog.list()[["name", "active"]]
              name  active
        0  grocery    True

        See Also
        --------
        - [Catalog.create](/reference/featurebyte.api.catalog.Catalog.create/): Create Catalog
        """
        try:
            catalog = Catalog.get(name=name)
            activate_catalog(catalog.id)
            return catalog
        except RecordRetrievalException:
            return Catalog.create(
                name=name,
                feature_store_name=feature_store_name,
                online_store_name=online_store_name,
            )

    @classmethod
    def get_active(cls) -> Optional[Catalog]:
        """
        Gets the currently active catalog.

        Returns
        -------
        Optional[Catalog]
            The currently active catalog or None if no catalog is active.

        Examples
        --------
        >>> catalog = fb.Catalog.get_active()
        >>> catalog.name
        'grocery'
        """
        active_catalog_id = get_active_catalog_id()
        if not active_catalog_id:
            return None
        return cls.get_by_id(active_catalog_id)

    @classmethod
    def _list_handler(cls) -> ListHandler:
        return CatalogListHandler(
            route=cls._route,
            list_schema=cls._list_schema,
            list_fields=cls._list_fields,
            list_foreign_keys=cls._list_foreign_keys,
        )

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

    @typechecked
    def update_online_store(self, online_store_name: Optional[str]) -> None:
        """
        Updates online store for the catalog.

        Parameters
        ----------
        online_store_name: Optional[str]
            Name of online store to use, or None to disable online serving for the catalog.

        Examples
        --------
        >>> catalog = fb.Catalog.get_active()
        >>> catalog.update_online_store("mysql_online_store")
        >>> catalog.online_store.name
        'mysql_online_store'
        """
        assert self.saved, "Catalog must be saved before updating online store"
        if not online_store_name:
            online_store_id = None
        else:
            online_store = OnlineStore.get(online_store_name)
            online_store_id = str(online_store.id)
        self.patch_async_task(
            route=f"{self._route}/{self.id}/online_store_async",
            payload=CatalogOnlineStoreUpdate(online_store_id=online_store_id).json_dict(),
        )
        # call get to update the object cache
        self._get_by_id(self.id, use_cache=False)

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
    def get(cls, name: str) -> Catalog:
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

        >>> catalog = fb.Catalog.get(<catalog_name>)  # doctest: +SKIP
        """
        return super().get(name)

    @classmethod
    def get_by_id(cls, id: ObjectId) -> Catalog:
        """
        Returns a Catalog object by its unique identifier (ID).

        Parameters
        ----------
        id: ObjectId
            Catalog unique identifier ID.

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
    def list(cls, include_id: Optional[bool] = True) -> pd.DataFrame:
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
            Entity name.
        serving_names: List[str]
            List of accepted names for the unique identifier that is used to identify the entity during a preview
            or serving request.

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
        List all features saved in the catalog.

        >>> features = catalog.list_features()


        List all features having grocerycustomer or frenchstate as primary entity.

        >>> customer_or_state_features = catalog.list_features(
        ...     primary_entity=["grocerycustomer", "frenchstate"]
        ... )
        """
        return Feature.list(
            include_id=include_id, primary_entity=primary_entity, primary_table=primary_table
        )

    @update_and_reset_catalog
    def list_feature_lists(
        self,
        include_id: Optional[bool] = True,
        primary_entity: Optional[Union[str, List[str]]] = None,
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
        primary_entity: Optional[Union[str, List[str]]] = None,
            Specifies the primary entity or entities for filtering results. Retrieve feature lists with the
            specified primary entity name(s). If multiple entities are provided in a list, the filtered
            results will include feature lists that have primary entities matching any entity in this list.
        entity: Optional[str]
            Specifies the entity name for filtering results. Retrieve feature lists containing features
            with the specified entity name as their primary entity.
        table: Optional[str]
            Specifies the table name for filtering results. Retrieve feature lists associated with
            the specified table name.

        Returns
        -------
        pd.DataFrame
            Table of feature lists.

        Examples
        --------
        >>> feature_lists = catalog.list_feature_lists()
        """
        return FeatureList.list(
            include_id=include_id, primary_entity=primary_entity, entity=entity, table=table
        )

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
        >>> tables = catalog.list_tables()
        """
        return Table.list(include_id=include_id, entity=entity)

    @update_and_reset_catalog
    def list_targets(self, include_id: Optional[bool] = True) -> pd.DataFrame:
        """
        Returns a DataFrame that contains various attributes of the registered targets in the catalog

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.

        Returns
        -------
        pd.DataFrame
            Dataframe of targets

        Examples
        --------
        >>> targets = catalog.list_targets()
        """
        return Target.list(include_id=include_id)

    @update_and_reset_catalog
    def list_use_cases(self, include_id: Optional[bool] = True) -> pd.DataFrame:
        """
        Returns a DataFrame that contains various attributes of the registered use cases in the catalog

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.

        Returns
        -------
        pd.DataFrame
            Dataframe of use cases

        Examples
        --------
        >>> use_cases = catalog.list_use_cases()
        """
        return UseCase.list(include_id=include_id)

    @update_and_reset_catalog
    def list_contexts(self, include_id: Optional[bool] = True) -> pd.DataFrame:
        """
        Returns a DataFrame that contains various attributes of the registered contexts in the catalog

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.

        Returns
        -------
        pd.DataFrame
            Dataframe of contexts

        Examples
        --------
        >>> contexts = catalog.list_contexts()
        """

        return Context.list(include_id=include_id)

    @update_and_reset_catalog
    def list_relationships(
        self,
        include_id: Optional[bool] = True,
        relationship_type: Optional[Union[RelationshipType, str]] = None,
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
            Whether to include Relationship object id in the output.
        relationship_type: Optional[Union[RelationshipType, str]]
            This parameter allows you to filter the results based on a specific relationship type. If no relationship
            type is provided (default is None), no filtering will be applied based on relationship type.

        Returns
        -------
        pd.DataFrame
            A dataframe containing the relationships.

        Examples
        --------
        List all relationships.

        >>> catalog.list_relationships()[
        ...     [
        ...         "relationship_type",
        ...         "entity",
        ...         "related_entity",
        ...     ]
        ... ]
          relationship_type           entity   related_entity
        0      child_parent   groceryinvoice  grocerycustomer
        1      child_parent  grocerycustomer      frenchstate
        """
        type_value = None
        if relationship_type:
            type_value = RelationshipType(relationship_type).value
        return Relationship.list(include_id=include_id, relationship_type=type_value)

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
    def list_online_stores(self, include_id: Optional[bool] = True) -> pd.DataFrame:
        """
        List saved online stores.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.

        Returns
        -------
        pd.DataFrame
            Table of online stores

        Examples
        --------
        List saved online stores.

        >>> online_stores = catalog.list_online_stores()
        """
        return OnlineStore.list(include_id=include_id)

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
    def list_batch_request_tables(self, include_id: Optional[bool] = True) -> pd.DataFrame:
        """
        List saved batch request tables.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.

        Returns
        -------
        pd.DataFrame
            Table of batch request tables.

        Examples
        --------
        List saved batch request tables.

        >>> batch_request_tables = catalog.list_batch_request_tables()
        """
        return BatchRequestTable.list(include_id=include_id)

    @update_and_reset_catalog
    def list_batch_feature_tables(self, include_id: Optional[bool] = True) -> pd.DataFrame:
        """
        List saved batch feature tables.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.

        Returns
        -------
        pd.DataFrame
            Table of batch feature tables.

        Examples
        --------
        List saved batch feature tables.

        >>> batch_feature_tables = catalog.list_batch_feature_tables()
        """
        return BatchFeatureTable.list(include_id=include_id)

    @update_and_reset_catalog
    def list_static_source_tables(self, include_id: Optional[bool] = True) -> pd.DataFrame:
        """
        List saved static source tables.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.

        Returns
        -------
        pd.DataFrame
            Table of static source tables.

        Examples
        --------
        List saved static source tables.

        >>> static_source_tables = catalog.list_static_source_tables()
        """
        return StaticSourceTable.list(include_id=include_id)

    @update_and_reset_catalog
    def list_deployments(
        self,
        include_id: Optional[bool] = True,
        feature_list_id: Optional[Union[ObjectId, str]] = None,
    ) -> pd.DataFrame:
        """
        List saved deployments.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.
        feature_list_id: Optional[Union[ObjectId, str]]
            Filter deployments by feature list ID.

        Returns
        -------
        pd.DataFrame
            Table of deployments.

        Examples
        --------
        List saved deployments.

        >>> deployments = catalog.list_deployments()
        """
        return Deployment.list(include_id=include_id, feature_list_id=feature_list_id)

    @update_and_reset_catalog
    def list_user_defined_functions(self, include_id: Optional[bool] = True) -> pd.DataFrame:
        """
        List saved user defined functions.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list.

        Returns
        -------
        pd.DataFrame
            Table of user defined functions.

        Examples
        --------
        List saved user defined functions.

        >>> user_defined_functions = catalog.list_user_defined_functions()
        """
        return UserDefinedFunction.list(include_id=include_id)

    @update_and_reset_catalog
    def get_data_source(self) -> DataSource:
        """
        Gets the data source from the catalog to access source tables from the data warehouse.

        Returns
        -------
        DataSource
            Data source object

        Examples
        --------
        >>> data_source = catalog.get_data_source()
        """
        assert (
            len(self.internal_default_feature_store_ids) == 1
        ), "No active catalog in this session. Please activate an existing catalog or create a new one to proceed."
        feature_store = FeatureStore.get_by_id(id=self.internal_default_feature_store_ids[0])
        return feature_store.get_data_source()

    @update_and_reset_catalog
    def get_view(self, table_name: str) -> View:
        """
        Gets a View object from the catalog based on the name of the table the view should be derived from.

        The method doesn’t support the manual mode. If you want to create a view in manual mode, first obtain the table
        and derive the view from the table.

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
        Get a default version of a feature by its name space.

        >>> feature = catalog.get_feature("InvoiceAmountAvg_60days")

        Get a specific version of a feature by specifying its version name.

        >>> feature = catalog.get_feature(  # doctest: +SKIP
        ...   "InvoiceAmountAvg_60days", version=<version_name>
        ... )
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
        >>> item_table = catalog.get_table("INVOICEITEMS")
        """
        return Table.get(name=name)

    @update_and_reset_catalog
    def get_target(self, name: str) -> Any:
        """
        Gets a Target object from the catalog based on its name.

        Parameters
        ----------
        name: str
            Target name.

        Returns
        -------
        Any
            Retrieved target.

        Examples
        --------
        >>> target = catalog.get_target("target_name")  # doctest: +SKIP
        """
        return Target.get(name=name)

    @update_and_reset_catalog
    def get_use_case(self, name: str) -> UseCase:
        """
        Gets a UseCase object from the catalog based on its name.

        Parameters
        ----------
        name: str
            UseCase name.

        Returns
        -------
        UseCase
            Retrieved UseCase.

        Examples
        --------
        Get a saved UseCase.

        >>> use_case = catalog.get_use_case("use_case_name")  # doctest: +SKIP
        """
        return UseCase.get(name=name)

    @update_and_reset_catalog
    def get_context(self, name: str) -> Any:
        """
        Gets a Context object from the catalog based on its name.

        Parameters
        ----------
        name: str
            Context name.

        Returns
        -------
        Any
            Retrieved Context.

        Examples
        --------
        Get a saved Context.

        >>> context = catalog.get_context("context_name")  # doctest: +SKIP
        """
        return Context.get(name=name)

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
    def get_deployment(self, name: str) -> Deployment:
        """
        Gets a Deployment object by name.

        Parameters
        ----------
        name: str
            Deployment name.

        Returns
        -------
        Deployment
            Deployment object.

        Examples
        --------
        Get a saved deployment.

        >>> deployment = catalog.get_deployment("deployment_name")  # doctest: +SKIP
        """
        return Deployment.get(name=name)

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
        >>> feature_job_setting_analysis = catalog.get_feature_job_setting_analysis(
        ...     "analysis_name"
        ... )  # doctest: +SKIP
        """
        return FeatureJobSettingAnalysis.get(name=name)

    @update_and_reset_catalog
    def get_feature_store(self, name: str) -> FeatureStore:
        """
        Get feature store by name.

        Parameters
        ----------
        name: str
            Name of the feature store to retrieve.

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
    def get_online_store(self, name: str) -> OnlineStore:
        """
        Get online store by name.

        Parameters
        ----------
        name: str
            Name of the online store to retrieve.

        Returns
        -------
        OnlineStore
            Online store object.

        Examples
        --------
        Get a saved online store.

        >>> online_store = catalog.get_online_store("mysql_online_store")
        """
        return OnlineStore.get(name=name)

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

        >>> observation_table = catalog.get_observation_table(
        ...     "observation_table_name"
        ... )  # doctest: +SKIP
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

        >>> historical_feature_table = catalog.get_historical_feature_table(
        ...     "historical_feature_table_name"
        ... )  # doctest: +SKIP
        """
        return HistoricalFeatureTable.get(name=name)

    @update_and_reset_catalog
    def get_batch_request_table(self, name: str) -> BatchRequestTable:
        """
        Get batch request table by name.

        Parameters
        ----------
        name: str
            Batch request table name.

        Returns
        -------
        BatchRequestTable
            Batch request table object.

        Examples
        --------
        Get a saved batch request table.

        >>> batch_request_table = catalog.get_batch_request_table(
        ...     "batch_request_table_name"
        ... )  # doctest: +SKIP
        """
        return BatchRequestTable.get(name=name)

    @update_and_reset_catalog
    def get_batch_feature_table(self, name: str) -> BatchFeatureTable:
        """
        Get batch feature table by name.

        Parameters
        ----------
        name: str
            Batch feature table name.

        Returns
        -------
        BatchFeatureTable
            Batch feature table object.

        Examples
        --------
        Get a saved batch feature table.

        >>> batch_feature_table = catalog.get_batch_feature_table(
        ...     "batch_feature_table_name"
        ... )  # doctest: +SKIP
        """
        return BatchFeatureTable.get(name=name)

    @update_and_reset_catalog
    def get_static_source_table(self, name: str) -> StaticSourceTable:
        """
        Get static source table by name.

        Parameters
        ----------
        name: str
            Static source table name.

        Returns
        -------
        StaticSourceTable
            Static source table object.

        Examples
        --------
        Get a saved static source table.

        >>> static_source_table = catalog.get_static_source_table(
        ...     "static_source_table_name"
        ... )  # doctest: +SKIP
        """
        return StaticSourceTable.get(name=name)

    @update_and_reset_catalog
    def get_user_defined_function(self, name: str) -> UserDefinedFunction:
        """
        Get user defined function by name.

        Parameters
        ----------
        name: str
            User defined function name.

        Returns
        -------
        UserDefinedFunction
            User defined function object.

        Examples
        --------
        Get a saved user defined function.

        >>> user_defined_function = catalog.get_user_defined_function(
        ...     "user_defined_function_name"
        ... )  # doctest: +SKIP
        """
        return UserDefinedFunction.get(name=name)
