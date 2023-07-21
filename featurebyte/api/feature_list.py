"""
FeatureListVersion class
"""
# pylint: disable=too-many-lines
from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)

from http import HTTPStatus

import pandas as pd
from bson.objectid import ObjectId
from pydantic import Field, root_validator
from typeguard import typechecked

from featurebyte.api.api_handler.base import ListHandler
from featurebyte.api.api_handler.feature_list import FeatureListListHandler
from featurebyte.api.api_object import ApiObject
from featurebyte.api.api_object_util import ForeignKeyMapping
from featurebyte.api.base_table import TableApiObject
from featurebyte.api.entity import Entity
from featurebyte.api.feature import Feature
from featurebyte.api.feature_group import BaseFeatureGroup, FeatureGroup, Item
from featurebyte.api.feature_job import FeatureJobMixin, FeatureJobStatusResult
from featurebyte.api.historical_feature_table import HistoricalFeatureTable
from featurebyte.api.observation_table import ObservationTable
from featurebyte.api.savable_api_object import DeletableApiObject, SavableApiObject
from featurebyte.common.descriptor import ClassInstanceMethodDescriptor
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.utils import (
    convert_to_list_of_strings,
    dataframe_to_arrow_bytes,
    enforce_observation_set_row_order,
)
from featurebyte.config import Configurations
from featurebyte.enum import ConflictResolution
from featurebyte.exception import RecordCreationException, RecordRetrievalException
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.base import PydanticObjectId, get_active_catalog_id
from featurebyte.models.feature_list import (
    FeatureListModel,
    FeatureListStatus,
    FeatureReadinessDistribution,
    FrozenFeatureListNamespaceModel,
)
from featurebyte.models.feature_namespace import DefaultVersionMode
from featurebyte.models.tile import TileSpec
from featurebyte.schema.deployment import DeploymentCreate
from featurebyte.schema.feature_list import (
    FeatureListCreate,
    FeatureListGetHistoricalFeatures,
    FeatureListModelResponse,
    FeatureListUpdate,
    FeatureVersionInfo,
)
from featurebyte.schema.feature_list_namespace import (
    FeatureListNamespaceModelResponse,
    FeatureListNamespaceUpdate,
)
from featurebyte.schema.historical_feature_table import HistoricalFeatureTableCreate

if TYPE_CHECKING:
    from featurebyte.api.deployment import Deployment
else:
    Deployment = TypeVar("Deployment")


class FeatureListNamespaceListHandler(ListHandler):
    """
    Additional handling for feature list namespace.
    """

    def additional_post_processing(self, item_list: pd.DataFrame) -> pd.DataFrame:
        # add information about default feature list version
        feature_list_versions = FeatureList.list_versions(include_id=True)
        feature_lists = item_list.merge(
            feature_list_versions[["id", "online_frac", "deployed"]].rename(
                columns={"id": "default_feature_list_id"}
            ),
            on="default_feature_list_id",
        )

        # replace id with default_feature_list_id
        feature_lists["id"] = feature_lists["default_feature_list_id"]

        feature_lists["num_feature"] = feature_lists.feature_namespace_ids.apply(len)
        feature_lists["readiness_frac"] = feature_lists.readiness_distribution.apply(
            lambda readiness_distribution: FeatureReadinessDistribution(
                __root__=readiness_distribution
            ).derive_production_ready_fraction()
        )
        return feature_lists


class FeatureListNamespace(FrozenFeatureListNamespaceModel, ApiObject):
    """
    FeatureListNamespace represents all the versions of the FeatureList that have the same FeatureList name.

    For example, a user might have created a FeatureList called "my feature list". That feature list might in turn
    contain 2 features:
    - feature_1,
    - feature_2

    The FeatureListNamespace object is primarily concerned with keeping track of version changes to the feature list,
    and not so much the version of the features within. This means that if a user creates a new version of "my feature
    list", the feature list namespace will contain a reference to the two versions. A simplified model would look like

      feature_list_namespace = ["my feature list_v1", "my feature list_v2"]

    Even if a user saves a new version of the feature in the feature list (eg. feature_1_v2), the
    feature_list_namespace will not change.
    """

    # class variable
    _route = "/feature_list_namespace"
    _update_schema_class = FeatureListNamespaceUpdate

    _list_schema = FeatureListNamespaceModelResponse
    _get_schema = FeatureListNamespaceModelResponse
    _list_fields = [
        "name",
        "num_feature",
        "status",
        "deployed",
        "readiness_frac",
        "online_frac",
        "tables",
        "entities",
        "primary_entities",
        "created_at",
    ]
    _list_foreign_keys = [
        ForeignKeyMapping("entity_ids", Entity, "entities"),
        ForeignKeyMapping("table_ids", TableApiObject, "tables"),
        ForeignKeyMapping("primary_entity_ids", Entity, "primary_entities"),
    ]

    @property
    def feature_list_ids(self) -> List[PydanticObjectId]:
        """
        List of feature list IDs from the same feature list namespace

        Returns
        -------
        List[PydanticObjectId]
        """
        return self.cached_model.feature_list_ids

    @property
    def deployed_feature_list_ids(self) -> List[PydanticObjectId]:
        """
        List of deployed feature list IDs from the same feature list namespace

        Returns
        -------
        List[PydanticObjectId]
        """
        return self.cached_model.deployed_feature_list_ids

    @property
    def readiness_distribution(self) -> FeatureReadinessDistribution:
        """
        Feature readiness distribution of the default feature list of this feature list namespace

        Returns
        -------
        FeatureReadinessDistribution
        """
        return self.cached_model.readiness_distribution

    @property
    def default_feature_list_id(self) -> PydanticObjectId:
        """
        Default feature list ID of this feature list namespace

        Returns
        -------
        PydanticObjectId
        """
        return self.cached_model.default_feature_list_id

    @property
    def default_version_mode(self) -> DefaultVersionMode:
        """
        Default feature list version mode of this feature list namespace

        Returns
        -------
        DefaultVersionMode
        """
        return self.cached_model.default_version_mode

    @property
    def status(self) -> FeatureListStatus:
        """
        Feature list status

        Returns
        -------
        FeatureListStatus

        Examples
        --------
        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> feature_list.status
        'TEMPLATE'
        """
        return self.cached_model.status

    @classmethod
    def _list_handler(cls) -> ListHandler:
        return FeatureListNamespaceListHandler(
            route=cls._route,
            list_schema=cls._list_schema,
            list_fields=cls._list_fields,
            list_foreign_keys=cls._list_foreign_keys,
        )

    @classmethod
    def list(
        cls,
        include_id: Optional[bool] = False,
        primary_entity: Optional[Union[str, List[str]]] = None,
        entity: Optional[str] = None,
        table: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        List saved feature lists

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        primary_entity: Optional[Union[str, List[str]]]
            Name of entity used to filter results. If multiple entities are provided, the filtered results will
            contain feature lists that are associated with all the entities.
        entity: Optional[str]
            Name of entity used to filter results
        table: Optional[str]
            Name of table used to filter results

        Returns
        -------
        pd.DataFrame
            Table of feature lists
        """
        feature_lists = super().list(include_id=include_id)
        target_entities = convert_to_list_of_strings(primary_entity)
        if target_entities:
            feature_lists = feature_lists[
                feature_lists.primary_entities.apply(
                    lambda primary_entities: set(target_entities).issubset(primary_entities)
                )
            ]
        if entity:
            feature_lists = feature_lists[
                feature_lists.entities.apply(lambda entities: entity in entities)
            ]
        if table:
            feature_lists = feature_lists[
                feature_lists.tables.apply(lambda table_list: table in table_list)
            ]
        return feature_lists


# pylint: disable=too-many-public-methods
class FeatureList(BaseFeatureGroup, DeletableApiObject, SavableApiObject, FeatureJobMixin):
    """
    The FeatureList class is used as a constructor to create a FeatureList Object.

    A FeatureList object is added to the catalog only when explicitly saved.

    A Feature List is a collection of Feature Objects specifically designed to address a particular Use Case. The
    Feature List is initially used to gather historical feature values for EDA, training, or testing data for a Use
    Case. These values are then utilized to analyze features, train, and test models. Once a model is trained and
    validated, the Feature List can be deployed, and the feature values can be accessed through online serving for
    generating predictions.

    Before serving a feature list, you can verify its primary entity using the primary_entity attribute. If the
    features within the list relate to different primary entities, the feature list's primary entity is determined
    based on the relationships between the entities, with the lowest-level entity chosen as the primary entity. If
    there are no relationships between entities, the primary entity may become a tuple comprising those entities.

    For example, imagine a feature list that includes features related to a card, customer, and customer city. In
    this scenario, the primary entity is the card entity because it is a child of both the customer and customer
    city entities. However, if the feature list also contains features for a merchant and merchant city, the primary
    entity becomes a tuple of card and merchant.

    Examples
    --------
    Create a feature list with two features.

    >>> features = fb.FeatureList([
    ...   catalog.get_feature("InvoiceCount_60days"),
    ...   catalog.get_feature("InvoiceAmountAvg_60days"),
    ... ], name="My new feature list")
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        proxy_class="featurebyte.FeatureList",
        hide_keyword_only_params_in_class_docs=True,
    )

    # class variables
    _route = "/feature_list"
    _update_schema_class = FeatureListUpdate
    _list_schema = FeatureListModelResponse
    _get_schema = FeatureListModelResponse
    _list_fields = [
        "name",
        "version",
        "num_feature",
        "online_frac",
        "deployed",
        "created_at",
        "is_default",
    ]

    # pydantic instance variable (internal use)
    internal_catalog_id: PydanticObjectId = Field(
        default_factory=get_active_catalog_id, alias="catalog_id"
    )
    internal_feature_ids: List[PydanticObjectId] = Field(alias="feature_ids", default_factory=list)

    @root_validator
    @classmethod
    def _initialize_feature_list_parameters(cls, values: dict[str, Any]) -> dict[str, Any]:
        # set the following values if it is empty (used mainly by the SDK constructed feature list)
        # for the feature list constructed during serialization, following codes should be skipped
        features = list(values["feature_objects"].values())
        values["internal_feature_ids"] = [feature.id for feature in features]
        return values

    @typechecked
    def __init__(self, items: Sequence[Item], name: str, **kwargs: Any):
        if "_id" in kwargs and "feature_ids" in kwargs:
            # FeatureList object constructed in SDK will not have _id & feature_ids attribute,
            # only the record retrieved from the persistent if kwargs contain these attributes.
            # Use this check to decide whether to make API call to retrieve features.
            items, feature_objects = self._initialize_items_and_feature_objects_from_persistent(
                feature_list_id=kwargs["_id"], feature_ids=kwargs["feature_ids"]
            )
            kwargs["feature_objects"] = feature_objects

        super().__init__(items=items, name=name, **kwargs)

    @property
    def version(self) -> str:
        """
        Returns the version identifier of a FeatureList object.

        Returns
        -------
        str

        Examples
        --------
        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> feature_list.version  # doctest: +SKIP
        'V230330'
        """
        return cast(FeatureListModel, self.cached_model).version.to_str()

    @property
    def catalog_id(self) -> ObjectId:
        """
        Returns the catalog ID that is associated with the FeatureList object.

        Returns
        -------
        ObjectId
            Catalog ID of the feature list.
        See Also
        --------
        - [Catalog](/reference/featurebyte.api.catalog.Catalog)
        """
        try:
            return cast(FeatureListModel, self.cached_model).catalog_id
        except RecordRetrievalException:
            return self.internal_catalog_id

    def _get_init_params_from_object(self) -> dict[str, Any]:
        return {"items": self.items}

    def _get_feature_tiles_specs(self) -> List[Tuple[str, List[TileSpec]]]:
        feature_tile_specs = []
        for feature in self.feature_objects.values():
            tile_specs = ExtendedFeatureModel(**feature.dict(by_alias=True)).tile_specs
            if tile_specs:
                feature_tile_specs.append((str(feature.name), tile_specs))
        return feature_tile_specs

    @typechecked
    def get_feature_jobs_status(
        self,
        job_history_window: int = 1,
        job_duration_tolerance: int = 60,
    ) -> FeatureJobStatusResult:
        """
        Returns a report on the recent activity of scheduled feature jobs associated with a FeatureList object.

        The report includes recent runs for these jobs, whether they were successful, and the duration of the jobs.
        This provides a summary of the health of the feature, and whether online features are updated in a timely
        manner.

        Failed and late jobs can occur due to various reasons, including insufficient compute capacity. Check your
        data warehouse logs for more details on the errors. If the errors are due to insufficient compute capacity,
        you can consider upsizing your instances.

        Parameters
        ----------
        job_history_window: int
            History window in hours.
        job_duration_tolerance: int
            Maximum duration before job is considered later, in seconds.

        Returns
        -------
        FeatureJobStatusResult

        Examples
        --------
        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> feature_list.get_feature_jobs_status()  # doctest: +SKIP
        """
        return super().get_feature_jobs_status(
            job_history_window=job_history_window, job_duration_tolerance=job_duration_tolerance
        )

    def info(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Returns a dictionary that summarizes the essential information of an FeatureList object. The dictionary
        contains the following keys:

        - `name`: The name of the FeatureList object.
        - `description`: The description of the FeatureList object.
        - `namespace_description`: The namespace description of the FeatureList object.
        - `created_at`: The timestamp indicating when the FeatureList object was created.
        - `updated_at`: The timestamp indicating when the FeatureList object was last updated.
        - `primary_entity`: Details about the primary entity of the FeatureList object.
        - `entities`: List of entities involved in the computation of the features contained in the FeatureList object.
        - `tables`: List of tables involved in the computation of the features contained in the FeatureList object.
        - `default_version_mode`: Indicates whether the default version mode is 'auto' or 'manual'.
        - `version_count`: The number of versions with the same feature list namespace.
        - `catalog_name`: The catalog name of the FeatureList object.
        - `status`: The status of the FeatureList object.
        - `feature_count`: The number of features contained in the FeatureList object.
        - `dtype_distribution`: The number of features per data type.
        - `deployed`: Indicates whether the FeatureList object is deployed
        - `serving_endpoint`: The URL for a deployed FeatureList for online serving of features.

        Some information is provided for both the FeatureList object and the default version with the same feature
        list namespace:

        - `version`: The version name.
        - `production_ready_fraction`: The percentage of features that are production-ready.
        - `default_feature_fraction`: The percentage of features that are default features.

        This method is only available for FeatureList objects that are saved in the catalog.

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
        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> info = feature_list.info()
        >>> del info["created_at"]
        >>> del info["updated_at"]
        >>> info  # doctest: +ELLIPSIS
        {
          'name': 'invoice_feature_list',
          'description': None,
          'entities': [
            {
              'name': 'grocerycustomer',
              'serving_names': [
                'GROCERYCUSTOMERGUID'
              ],
              'catalog_name': 'grocery'
            }
          ],
          'primary_entity': [
            {
              'name': 'grocerycustomer',
              'serving_names': [
                'GROCERYCUSTOMERGUID'
              ],
              'catalog_name': 'grocery'
            }
          ],
          'tables': [
            {
              'name': 'GROCERYINVOICE',
              'status': 'PUBLIC_DRAFT',
              'catalog_name': 'grocery'
            }
          ],
          'default_version_mode': 'AUTO',
          'version_count': 3,
          'catalog_name': 'grocery',
          'dtype_distribution': [
            {
              'dtype': 'FLOAT',
              'count': 1
            }
          ],
          'default_feature_list_id': ...,
          'status': 'DRAFT',
          'feature_count': 1,
          'version': {
            'this': ...,
            'default': ...
          },
          'production_ready_fraction': {
            'this': 1.0,
            'default': 1.0
          },
          'default_feature_fraction': {
            'this': 1.0,
            'default': 1.0
          },
          'versions_info': None,
          'deployed': False,
          'namespace_description': None
        }
        """
        return super().info(verbose)

    @property
    def feature_names(self) -> list[str]:
        """
        Returns a report on the recent activity of scheduled feature jobs associated with a FeatureList object.

        The report includes recent runs for these jobs, whether they were successful, and the duration of the jobs.
        This provides a summary of the health of the features of the feature list, and whether online features are
        updated in a timely manner.

        Failed and late jobs can occur due to various reasons, including insufficient compute capacity. Check your
        data warehouse logs for more details on the errors. If the errors are due to insufficient compute capacity,
        you can consider upsizing your instances.

        Returns
        -------
        list[str]
            List of feature names

        Examples
        --------
        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> feature_list.feature_names
        ['InvoiceCount_60days']

        See Also
        --------
        - [FeatureGroup.feature_names](/reference/featurebyte.api.feature_group.FeatureGroup.feature_names/)
        """
        return super().feature_names

    @property
    def feature_ids(self) -> Sequence[ObjectId]:
        """
        Returns the unique identifier (ID) of the Feature objects associated with the FeatureList object.

        Returns
        -------
        Sequence[ObjectId]
        """
        try:
            return cast(FeatureListModel, self.cached_model).feature_ids
        except RecordRetrievalException:
            return self.internal_feature_ids

    @classmethod
    def _get_init_params(cls) -> dict[str, Any]:
        return {"items": []}

    @classmethod
    def get(cls, name: str, version: Optional[str] = None) -> FeatureList:
        """
        Retrieve the FeatureList from the persistent data store given the object's name, and version.

        This assumes that the object has been saved to the persistent data store. If the object has not been saved,
        an exception will be raised. To fix this, you should save the object first.

        Parameters
        ----------
        name: str
            Name of the FeatureList to retrieve.
        version: Optional[str]
            FeatureList version, if None, the default version will be returned.

        Returns
        -------
        FeatureList
            FeatureList object.

        Examples
        --------
        Get a FeatureList object that is already saved.

        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        """
        if version is None:
            feature_list_namespace = FeatureListNamespace.get(name=name)
            return cls.get_by_id(id=feature_list_namespace.default_feature_list_id)
        return cls._get(name=name, other_params={"version": version})

    def _get_create_payload(self) -> dict[str, Any]:
        feature_ids = [feature.id for feature in self.feature_objects.values()]
        data = FeatureListCreate(
            **{**self.dict(by_alias=True, exclude_none=True), "feature_ids": feature_ids}
        )
        return data.json_dict()

    def save(
        self, conflict_resolution: ConflictResolution = "raise", _id: Optional[ObjectId] = None
    ) -> None:
        """
        Adds a FeatureList object to the catalog.

        A conflict could be triggered when the object being saved has violated a uniqueness check at the catalog.
        If uniqueness is violated, you can either raise an error or retrieve the object with the same name, depending
        on the conflict resolution parameter passed in. The default behavior is to raise an error.

        Parameters
        ----------
        conflict_resolution: ConflictResolution
            "raise" raises error when we encounter a conflict error (default).
            "retrieve" handle conflict error by retrieving the object with the same name.
        _id: Optional[ObjectId]
            The object ID to be used when saving the object. If not provided, a new object ID will be generated.

        Examples
        --------
        >>> feature_list = fb.FeatureList([
        ...     catalog.get_feature("InvoiceCount_60days"),
        ...     catalog.get_feature("InvoiceAmountAvg_60days"),
        ... ], name="feature_lists_invoice_features")
        >>> feature_list.save()  # doctest: +SKIP
        """
        assert self.name is not None, "FeatureList name cannot be None"
        self._check_object_not_been_saved(conflict_resolution=conflict_resolution)

        # prepare feature list batch feature create payload
        feature_list_batch_feature_create = self._get_feature_list_batch_feature_create_payload(
            feature_list_id=self.id,
            feature_list_name=self.name,
            conflict_resolution=conflict_resolution,
        )
        self.post_async_task(
            route="/feature_list/batch",
            payload=feature_list_batch_feature_create.json_dict(),
            retrieve_result=False,
            has_output_url=False,
        )
        object_dict = self._get_object_dict_by_id(id_value=feature_list_batch_feature_create.id)
        type(self).__init__(self, **object_dict, **self._get_init_params_from_object())

    def delete(self) -> None:
        """
        Deletes a FeatureList object from the persistent data store. A feature list can only be deleted from the
        persistent data store if:

        - the feature list status is DRAFT
        - the feature list is not a default feature list with manual version mode

        Examples
        --------
        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> feature_list.delete()  # doctest: +SKIP
        """
        self._delete()

    @typechecked
    def drop(self, items: List[str]) -> FeatureGroup:
        """
        Drops feature(s) from the original FeatureList and returns a new FeatureGroup object.

        Parameters
        ----------
        items: List[str]
            List of feature names to be dropped

        Returns
        -------
        FeatureGroup
            FeatureGroup object containing remaining feature(s)

        Examples
        --------
        >>> feature_list = fb.FeatureList([
        ...     catalog.get_feature("InvoiceCount_60days"),
        ...     catalog.get_feature("InvoiceAmountAvg_60days"),
        ... ], name="feature_lists_invoice_features")
        >>> amount_only_feature_list = feature_list.drop(["InvoiceCount_60days"])
        """
        return super().drop(items=items)

    @property
    def saved(self) -> bool:
        """
        Returns whether the FeatureList object is saved and added to the catalog.

        Returns
        -------
        bool
        """
        return super().saved

    @typechecked
    def preview(
        self,
        observation_set: pd.DataFrame,
    ) -> Optional[pd.DataFrame]:
        """
        Materializes a FeatureList using a small observation set of up to 50 rows. Unlike compute_historical_features,
        this method does not store partial aggregations (tiles) to speed up future computation. Instead, it computes
        the features on the fly, and should be used only for small observation sets for debugging or prototyping
        unsaved features.

        The small observation set should combine historical points-in-time and key values of the primary entity from
        the feature list. Associated serving entities can also be utilized.

        Parameters
        ----------
        observation_set : pd.DataFrame
            Observation set DataFrame which combines historical points-in-time and values of the feature primary entity
            or its descendant (serving entities). The column containing the point-in-time values should be named
            `POINT_IN_TIME`, while the columns representing entity values should be named using accepted serving
            names for the entity.

        Returns
        -------
        pd.DataFrame
            Materialized feature values.
            The returned DataFrame will have the same number of rows, and include all columns from the observation set.

            **Note**: `POINT_IN_TIME` values will be converted to UTC time.

        Examples
        --------
        Create a feature list with two features.

        >>> features = fb.FeatureList([
        ...    catalog.get_feature("InvoiceCount_60days"),
        ...    catalog.get_feature("InvoiceAmountAvg_60days"),
        ... ], name="My new feature list")


        Prepare observation set with POINT_IN_TIME and serving names columns.

        >>> observation_set = pd.DataFrame({
        ...    "POINT_IN_TIME": ["2022-06-01 00:00:00", "2022-06-02 00:00:00"],
        ...    "GROCERYCUSTOMERGUID": [
        ...      "a2828c3b-036c-4e2e-9bd6-30c9ee9a20e3",
        ...      "ac479f28-e0ff-41a4-8e60-8678e670e80b",
        ...    ],
        ... })


        Preview the feature list with a small observation set.

        >>> features.preview(observation_set)
            POINT_IN_TIME  GROCERYCUSTOMERGUID                   InvoiceCount_60days  InvoiceAmountAvg_60days
        0   2022-06-01     a2828c3b-036c-4e2e-9bd6-30c9ee9a20e3  10.0                 7.938
        1   2022-06-02     ac479f28-e0ff-41a4-8e60-8678e670e80b  6.0                  9.870
        """
        return super().preview(observation_set=observation_set)

    @property
    def feature_list_namespace(self) -> FeatureListNamespace:
        """
        FeatureListNamespace object of current feature list

        Returns
        -------
        FeatureListNamespace
        """
        feature_list_namespace_id = cast(
            FeatureListModel, self.cached_model
        ).feature_list_namespace_id
        return FeatureListNamespace.get_by_id(id=feature_list_namespace_id)

    @property
    def online_enabled_feature_ids(self) -> List[PydanticObjectId]:
        """
        List of online enabled feature IDs of this feature list

        Returns
        -------
        List[PydanticObjectId]
        """
        try:
            return self.cached_model.online_enabled_feature_ids
        except RecordRetrievalException:
            return sorted(
                feature.id for feature in self.feature_objects.values() if feature.online_enabled
            )

    @property
    def readiness_distribution(self) -> FeatureReadinessDistribution:
        """
        Feature readiness distribution of this feature list

        Returns
        -------
        FeatureReadinessDistribution
        """
        try:
            return self.cached_model.readiness_distribution
        except RecordRetrievalException:
            return FeatureListModel.derive_readiness_distribution(
                list(self.feature_objects.values())  # type: ignore
            )

    @property
    def production_ready_fraction(self) -> float:
        """
        Retrieve fraction of production ready features in the feature list

        Returns
        -------
        Fraction of production ready feature

        See Also
        --------
        - [FeatureList.info](/reference/featurebyte.api.feature_list.FeatureList.info/)
        """
        return self.readiness_distribution.derive_production_ready_fraction()

    @property
    def default_feature_fraction(self) -> float:
        """
        Retrieve fraction of default features in the feature list

        Returns
        -------
        Fraction of default feature

        See Also
        --------
        - [Feature.info](/reference/featurebyte.api.feature.Feature.info/)
        - [Feature.is_default](/reference/featurebyte.api.feature.Feature.is_default/)
        - [FeatureList.info](/reference/featurebyte.api.feature_list.FeatureList.info/)
        """
        namespace_info = self.feature_list_namespace.info()
        default_feat_ids = set(namespace_info["default_feature_ids"])
        default_feat_count = 0
        for feat_id in self.feature_ids:
            if str(feat_id) in default_feat_ids:
                default_feat_count += 1
        return default_feat_count / len(self.feature_ids)

    @property
    def deployed(self) -> bool:
        """
        Whether this feature list is deployed or not

        Returns
        -------
        bool
        """
        try:
            return self.cached_model.deployed
        except RecordRetrievalException:
            return False

    @property
    def is_default(self) -> bool:
        """
        Check whether current feature list is the default one or not

        Returns
        -------
        bool
        """
        return self.id == self.feature_list_namespace.default_feature_list_id

    @property
    def default_version_mode(self) -> DefaultVersionMode:
        """
        Retrieve default version mode of current feature list namespace

        Returns
        -------
        DefaultVersionMode
        """
        return self.feature_list_namespace.default_version_mode

    @property
    def status(self) -> FeatureListStatus:
        """
        Retrieve feature list status at persistent

        Returns
        -------
        Feature list status
        """
        return self.feature_list_namespace.status

    @classmethod
    def _list_handler(cls) -> ListHandler:
        return FeatureListListHandler(
            route=cls._route,
            list_schema=cls._list_schema,
            list_fields=cls._list_fields,
            list_foreign_keys=cls._list_foreign_keys,
        )

    @classmethod
    def _list_versions(cls, include_id: Optional[bool] = True) -> pd.DataFrame:
        """
        Returns a DataFrame that presents a summary of the feature list versions belonging to the namespace of the
        FeatureList object. The DataFrame contains multiple attributes of the feature list versions, such as their
        versions names, deployment states, creation dates and the percentage of their features that are
        production_ready.

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include FeatureList object id in the output table.

        Returns
        -------
        pd.DataFrame
            Table of feature lists

        Examples
        --------
        List saved FeatureList versions (calling from FeatureList class):

        >>> FeatureList.list_versions(include_id=False)  # doctest: +ELLIPSIS
                           name  version  num_feature  online_frac  deployed  created_at  is_default
        0  invoice_feature_list      ...            1          0.0     False         ...  True

        List FeatureList versions with the same name (calling from FeatureList object):

        >>> feature_list = catalog.get_feature_list("invoice_feature_list")  # doctest: +ELLIPSIS
        >>> feature_list.list_versions(include_id=False)  # doctest: +ELLIPSIS
                           name  version  online_frac  deployed created_at  is_default
        0  invoice_feature_list      ...          0.0     False        ...        True

        See Also
        --------
        - [FeatureList.list_features](/reference/featurebyte.api.feature_list.FeatureList.list_features/)
        """
        return super().list(include_id=include_id)

    def _list_versions_with_same_name(self, include_id: bool = True) -> pd.DataFrame:
        """
        List feature list versions with the same name

        Parameters
        ----------
        include_id: bool
            Whether to include id in the list

        Returns
        -------
        pd.DataFrame
            Table of features with the same name

        Examples
        --------
        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> feature_list.list_versions(include_id=False)  # doctest: +ELLIPSIS
                           name  version  online_frac  deployed  created_at  is_default
        0  invoice_feature_list      ...          0.0     False         ...        True
        """
        output = self._list(include_id=True, params={"name": self.name})
        default_feature_list_id = self.feature_list_namespace.default_feature_list_id
        output["is_default"] = output["id"] == default_feature_list_id
        exclude_cols = {"num_feature"}
        if not include_id:
            exclude_cols.add("id")
        return output[[col for col in output.columns if col not in exclude_cols]]

    @classmethod
    def list(
        cls,
        include_id: Optional[bool] = False,
        primary_entity: Optional[Union[str, List[str]]] = None,
        entity: Optional[str] = None,
        table: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        List saved feature lists

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        primary_entity: Optional[Union[str, List[str]]] = None,
            Name of entity used to filter results. If multiple entities are provided, the filtered results will
            contain feature lists that are associated with all the entities.
        entity: Optional[str]
            Name of entity used to filter results
        table: Optional[str]
            Name of table used to filter results

        Returns
        -------
        pd.DataFrame
            Table of feature lists
        """
        return FeatureListNamespace.list(
            include_id=include_id, primary_entity=primary_entity, entity=entity, table=table
        )

    def list_features(self) -> pd.DataFrame:
        """
        Returns a DataFrame that contains various attributes of the features in a Feature List object, such as their
        names, versions, types, corresponding tables, related entities, creation dates, states of readiness and
        online availability.

        Returns
        -------
        pd.DataFrame
            Table of features in this feature list.

        Examples
        --------
        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> display_columns = ["name", "version", "dtype", "primary_tables", "primary_entities"]
        >>> feature_list.list_features().sort_values("created_at")[display_columns]  # doctest: +SKIP
                          name  version  dtype    primary_tables   primary_entities
        0  InvoiceCount_60days  V230330  FLOAT  [GROCERYINVOICE]  [grocerycustomer]

        See Also
        --------
        - [FeatureList.list_versions](/reference/featurebyte.api.feature_list.FeatureList.list_versions/)
        """
        return Feature.list_versions(feature_list_id=self.id)

    @typechecked
    def get_historical_features_sql(
        self,
        observation_set: pd.DataFrame,
        serving_names_mapping: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Retrieve partial SQL statements used to retrieved historical features (for debugging / understanding purposes).

        Parameters
        ----------
        observation_set : pd.DataFrame
            Observation set DataFrame, which should contain the `POINT_IN_TIME` column,
            as well as columns with serving names for all entities used by features in the feature list.
        serving_names_mapping : Optional[Dict[str, str]]
            Optional serving names mapping if the training events table has different serving name
            columns than those defined in Entities. Mapping from original serving name to new
            serving name.

        Returns
        -------
        str
            Partial SQL statements used to retrieved historical features.

        Raises
        ------
        RecordRetrievalException
            Get historical features request failed.
        """
        payload = FeatureListGetHistoricalFeatures(
            feature_list_id=self.id,
            feature_clusters=self._get_feature_clusters(),
            serving_names_mapping=serving_names_mapping,
        )

        client = Configurations().get_client()
        response = client.post(
            "/feature_list/historical_features_sql",
            data={"payload": payload.json()},
            files={"observation_set": dataframe_to_arrow_bytes(observation_set)},
        )
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)

        return cast(
            str,
            response.json(),
        )

    @enforce_observation_set_row_order
    @typechecked
    def compute_historical_features(
        self,
        observation_set: pd.DataFrame,
        serving_names_mapping: Optional[Dict[str, str]] = None,
    ) -> pd.DataFrame:
        """
        Returns a DataFrame with feature values for analysis, model training, or evaluation. The historical features
        request data consists of an observation set that combines historical points-in-time and key values of the
        primary entity from the feature list.

        Associated serving entities can also be utilized.

        Initial computation might take more time, but following calls will be faster due to pre-computed and saved
        partially aggregated data (tiles).

        A training data observation set should typically meet the following criteria:

        * be collected from a time period that does not start until after the earliest data availability timestamp plus
        longest time window in the features
        * be collected from a time period that ends before the latest data timestamp less the time window of the target
        value
        * uses points in time that align with the anticipated timing of the use case inference, whether it's based on a
        regular schedule, triggered by an event, or any other timing mechanism.
        * does not have duplicate rows
        * has for the same entity, key points in time that have time intervals greater than the horizon of the target to
        avoid leakage.

        Parameters
        ----------
        observation_set : pd.DataFrame
            Observation set DataFrame or ObservationTable object, which combines historical points-in-time and values
            of the feature primary entity or its descendant (serving entities). The column containing the point-in-time
            values should be named `POINT_IN_TIME`, while the columns representing entity values should be named using
            accepted serving names for the entity.
        serving_names_mapping : Optional[Dict[str, str]]
            Optional serving names mapping if the training events table has different serving name columns than those
            defined in Entities, mapping from original serving name to new name.

        Returns
        -------
        pd.DataFrame
            Materialized historical features.

            **Note**: `POINT_IN_TIME` values will be converted to UTC time.

        Examples
        --------
        Create a feature list with two features.
        >>> feature_list = fb.FeatureList([
        ...     catalog.get_feature("InvoiceCount_60days"),
        ...     catalog.get_feature("InvoiceAmountAvg_60days"),
        ... ], name="InvoiceFeatures")

        Prepare observation set with POINT_IN_TIME and serving names columns.
        >>> observation_set = pd.DataFrame({
        ...     "POINT_IN_TIME": pd.date_range(start="2022-04-15", end="2022-04-30", freq="2D"),
        ...     "GROCERYCUSTOMERGUID": ["a2828c3b-036c-4e2e-9bd6-30c9ee9a20e3"] * 8,
        ... })

        Retrieve materialized historical features.
        >>> feature_list.compute_historical_features(observation_set)
          POINT_IN_TIME                   GROCERYCUSTOMERGUID  InvoiceCount_60days  InvoiceAmountAvg_60days
        0    2022-04-15  a2828c3b-036c-4e2e-9bd6-30c9ee9a20e3                  9.0                10.223333
        1    2022-04-17  a2828c3b-036c-4e2e-9bd6-30c9ee9a20e3                  9.0                10.223333
        2    2022-04-19  a2828c3b-036c-4e2e-9bd6-30c9ee9a20e3                  9.0                10.223333
        3    2022-04-21  a2828c3b-036c-4e2e-9bd6-30c9ee9a20e3                 10.0                 9.799000
        4    2022-04-23  a2828c3b-036c-4e2e-9bd6-30c9ee9a20e3                 10.0                 9.799000
        5    2022-04-25  a2828c3b-036c-4e2e-9bd6-30c9ee9a20e3                  9.0                 9.034444
        6    2022-04-27  a2828c3b-036c-4e2e-9bd6-30c9ee9a20e3                 10.0                 9.715000
        7    2022-04-29  a2828c3b-036c-4e2e-9bd6-30c9ee9a20e3                 10.0                 9.715000


        Retrieve materialized historical features with serving names mapping.
        >>> historical_features = feature_list.compute_historical_features(  # doctest: +SKIP
        ...   observation_set=observation_set,
        ...   serving_names_mapping={"GROCERYCUSTOMERGUID": "CUSTOMERGUID"}
        ... )

        See Also
        --------
        - [FeatureGroup.preview](/reference/featurebyte.api.feature_group.FeatureGroup.preview/):
          Preview feature group.
        - [Feature.preview](/reference/featurebyte.api.feature.Feature.preview/):
          Preview feature group.
        """
        temp_historical_feature_table_name = f"__TEMPORARY_HISTORICAL_FEATURE_TABLE_{ObjectId()}"
        temp_historical_feature_table = self.compute_historical_feature_table(
            observation_table=observation_set,
            historical_feature_table_name=temp_historical_feature_table_name,
            serving_names_mapping=serving_names_mapping,
        )
        try:
            return temp_historical_feature_table.to_pandas()
        finally:
            temp_historical_feature_table.delete()

    @typechecked
    def compute_historical_feature_table(
        self,
        observation_table: Union[ObservationTable, pd.DataFrame],
        historical_feature_table_name: str,
        serving_names_mapping: Optional[Dict[str, str]] = None,
    ) -> HistoricalFeatureTable:
        """
        Materialize feature list using an observation table asynchronously. The historical features
        will be materialized into a historical feature table.

        Parameters
        ----------
        observation_table: Union[ObservationTable, pd.DataFrame]
            Observation set with `POINT_IN_TIME` and serving names columns. This can be either an
            ObservationTable of a pandas DataFrame.
        historical_feature_table_name: str
            Name of the historical feature table to be created
        serving_names_mapping : Optional[Dict[str, str]]
            Optional serving names mapping if the training events table has different serving name

        Returns
        -------
        HistoricalFeatureTable

        Examples
        --------
        >>> # Get the desired observation table
        >>> observation_table = catalog.get_observation_table(<observation_table_name>)  # doctest: +SKIP
        >>> # Get the desired feature list
        >>> my_feature_list = catalog.get_feature_list(<feature_list_name>)  # doctest: +SKIP
        >>> # Decide the name of the historical feature table
        >>> training_table_name = (  # doctest: +SKIP
        ...   '2y Features for Customer Purchase next 2w '
        ...   'up to end 22 with Improved Feature List'
        ... )
        >>> # Compute the historical feature table
        >>> training_table = my_feature_list.compute_historical_feature_table(  # doctest: +SKIP
        ...   observation_table=observation_table,
        ...   historical_feature_table_name=training_table_name
        ...   serving_names_mapping={"GROCERYCUSTOMERGUID": "CUSTOMERGUID"}
        ... )
        """
        featurelist_get_historical_features = FeatureListGetHistoricalFeatures(
            feature_list_id=self.id,
            feature_clusters=self._get_feature_clusters(),
            serving_names_mapping=serving_names_mapping,
        )
        feature_store_id = featurelist_get_historical_features.feature_clusters[0].feature_store_id
        feature_table_create_params = HistoricalFeatureTableCreate(
            name=historical_feature_table_name,
            observation_table_id=(
                observation_table.id if isinstance(observation_table, ObservationTable) else None
            ),
            feature_store_id=feature_store_id,
            featurelist_get_historical_features=featurelist_get_historical_features,
        )
        if isinstance(observation_table, ObservationTable):
            files = None
        else:
            assert isinstance(observation_table, pd.DataFrame)
            files = {"observation_set": dataframe_to_arrow_bytes(observation_table)}
        historical_feature_table_doc = self.post_async_task(
            route="/historical_feature_table",
            payload={"payload": feature_table_create_params.json()},
            is_payload_json=False,
            files=files,
        )
        return HistoricalFeatureTable.get_by_id(historical_feature_table_doc["_id"])

    @typechecked
    def create_new_version(
        self, features: Optional[List[FeatureVersionInfo]] = None
    ) -> FeatureList:
        """
        Creates a new feature version from a FeatureList object. The current default version of the features within the
        feature list is employed to create the new version, except when specific versions are indicated by the
        feature's parameter.

        Parameters
        ----------
        features: Optional[List[FeatureVersionInfo]]
            List specific feature versions that must be used in the new FeatureList object instead of the default
            version. Each feature version in the list is defined by using the FeatureVersionInfo constructor that
            takes as input the feature name and the version.

        Returns
        -------
        FeatureList
            Newly created feature list with the specified features or the latest default versions of features.

        Raises
        ------
        RecordCreationException
            When failed to save a new version, e.g. when the created feature list is exactly the same as the current
            one. Another reason could be that the specified feature in `features` parameter does not exist.

        Examples
        --------
        Retrieve feature list & check its features.

        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> feature_list.list_features()[["name", "version"]]  # doctest: +SKIP
                          name  version
        0  InvoiceCount_60days  V230330

        Create a new feature by specifying the table name and feature job settings. Then set the newly created
        feature as default.

        >>> current_feature = feature_list["InvoiceCount_60days"]
        >>> new_feature = current_feature.create_new_version(
        ...   table_feature_job_settings=[
        ...     fb.TableFeatureJobSetting(
        ...       table_name="GROCERYINVOICE",
        ...       feature_job_setting=fb.FeatureJobSetting(
        ...         blind_spot="60s",
        ...         frequency="3600s",
        ...         time_modulo_frequency="90s",
        ...       )
        ...     )
        ...   ]
        ... )
        >>> current_feature.update_readiness("DEPRECATED")
        >>> new_feature.update_default_version_mode("MANUAL")
        >>> new_feature.as_default_version()
        >>> new_feature.is_default is True and current_feature.is_default is False
        True

        Create new version of feature list without specifying feature (uses the current default versions of feature).

        >>> new_feature_list = feature_list.create_new_version()
        >>> new_feature_list.list_features()[["name", "version"]]  # doctest: +SKIP
                          name    version
        0  InvoiceCount_60days  V230330_1

        Create new version of feature list by specifying feature:

        >>> new_feature_list = feature_list.create_new_version(
        ...   features=[fb.FeatureVersionInfo(name="InvoiceCount_60days", version=new_feature.version)]
        ... )

        Reset the default version mode of the feature to make original feature as default. Create a new version
        of feature list using original feature list should throw an error due to no change in feature list is detected.

        >>> current_feature.update_readiness("PRODUCTION_READY")
        >>> current_feature.update_default_version_mode("AUTO")
        >>> current_feature.is_default
        True
        >>> feature_list.create_new_version()  # doctest: +ELLIPSIS
        Traceback (most recent call last):
        ...
        RecordCreationException: No change detected on the new feature list version.

        See Also
        --------
        - [Feature.create_new_version](/reference/featurebyte.api.feature.Feature.create_new_version/):
        - [Feature.as_default_version](/reference/featurebyte.api.feature.Feature.as_default_version/):
        - [Feature.update_default_version_mode](/reference/featurebyte.api.feature.Feature.update_default_version_mode/):
        - [FeatureList.list_features](/reference/featurebyte.api.feature_list.FeatureList.list_features/):

        """
        client = Configurations().get_client()
        response = client.post(
            url=self._route,
            json={
                "source_feature_list_id": str(self.id),
                "features": [feature.dict() for feature in features] if features else [],
            },
        )
        if response.status_code != HTTPStatus.CREATED:
            raise RecordCreationException(response=response)
        return FeatureList(**response.json(), **self._get_init_params())

    @typechecked
    def update_status(
        self, status: Literal[tuple(FeatureListStatus)]  # type: ignore[misc]
    ) -> None:
        """
        A FeatureList can have one of five statuses:

        "DEPLOYED": Assigned to FeatureLists with at least one active version online.
        "TEMPLATE": For FeatureLists serving as reference templates or safe starting points.
        "PUBLIC DRAFT": For FeatureLists shared for feedback purposes.
        "DRAFT": For FeatureLists in the prototype stage.
        "DEPRECATED": For outdated or unneeded FeatureLists.

        Automatic status changes:
        - New FeatureLists are assigned the "DRAFT" status.
        - The "DEPLOYED" status is applied when at least one version is deployed.
        - If deployment is disabled for all versions, the FeatureList becomes a "PUBLIC_DRAFT".

        Additional guidelines:
        - Before making a FeatureList a "TEMPLATE", add a description and ensure all features are "production ready".
        - Only "DRAFT" FeatureLists can be deleted;
        - A FeatureList cannot be reverted to a "DRAFT" status.

        Parameters
        ----------
        status: Literal[tuple(FeatureListStatus)]
            Desired feature list status.

        Examples
        --------
        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> feature_list.update_status(fb.FeatureListStatus.TEMPLATE)
        """
        self.feature_list_namespace.update(
            update_payload={"status": str(status)}, allow_update_local=False
        )

    @typechecked
    def update_default_version_mode(
        self, default_version_mode: Literal[tuple(DefaultVersionMode)]  # type: ignore[misc]
    ) -> None:
        """
        Sets the default version mode of a feature list.

        By default, the feature list's default version mode is automatic, selecting the version with the highest
        percentage of production ready features. If several versions share the same readiness level, the most recent
        one becomes the default.

        If the default version mode is set as manual, you can choose to manually set any version as the default
        version for the feature list.

        Parameters
        ----------
        default_version_mode: Literal[tuple(DefaultVersionMode)]
            Feature list default version mode

        Examples
        --------
        >>> feature_list = catalog.get_feature_list_by_id(<FeatureList_Object_ID>)  # doctest: +SKIP
        >>> feature_list.update_default_version_mode(DefaultVersionMode.MANUAL)  # doctest: +SKIP

        See Also
        --------
        - [FeatureList.create_new_version](/reference/featurebyte.api.feature_list.FeatureList.create_new_version/)
        - [FeatureList.as_default_version](/reference/featurebyte.api.feature_list.FeatureList.as_default_version/)
        """
        self.feature_list_namespace.update(
            update_payload={"default_version_mode": DefaultVersionMode(default_version_mode).value},
            allow_update_local=False,
        )

    def as_default_version(self) -> None:
        """
        When a feature list has its default version mode set to manual, this method designates the FeatureList
        object as the default version for that specific feature list.

        Each feature list is recognized by its name and can possess numerous versions, though only a single default
        version is allowed.

        The default version streamlines feature list reuse by supplying the most suitable version when none is
        explicitly indicated. By default, the feature list's default version mode is automatic, selecting the version
        with the highest percentage of production ready features. If several versions share the same readiness level,
        the most recent one becomes the default."

        Examples
        --------
        >>> feature_list = catalog.get_feature_list_by_id(<FeatureList_Object_ID>)  # doctest: +SKIP
        >>> feature_list.update_default_version_mode(DefaultVersionMode.MANUAL)  # doctest: +SKIP
        >>> feature_list.as_default_version()  # doctest: +SKIP

        See Also
        --------
        - [FeatureList.create_new_version](/reference/featurebyte.api.feature_list.FeatureList.create_new_version/)
        - [FeatureList.update_default_version_mode](/reference/featurebyte.api.feature_list.FeatureList.update_default_version_mode/)
        """
        self.feature_list_namespace.update(
            update_payload={"default_feature_list_id": self.id}, allow_update_local=False
        )
        assert self.feature_list_namespace.default_feature_list_id == self.id

    @typechecked
    def deploy(
        self,
        deployment_name: Optional[str] = None,
        make_production_ready: bool = False,
        ignore_guardrails: bool = False,
    ) -> Deployment:
        """
        Create a deployment of a feature list. With a deployment, you can serve the feature list in production by
        either online or batch serving.

        This triggers the orchestration of the feature materialization into the online feature store. A feature list
        is deployed without creating separate pipelines or using different tools.

        Deployment can be disabled at any time if the serving of the feature list is not needed anymore. Unlike the
        log and wait approach adopted by some feature stores, disabling the deployment of a feature list doesn’t
        affect the serving of its historical requests.

        Parameters
        ----------
        deployment_name: Optional[str]
            Name of the deployment, if not provided, the name will be generated automatically.
        make_production_ready: bool
            Whether to convert the feature to production ready if it is not production ready.
        ignore_guardrails: bool
            Whether to ignore guardrails when trying to promote features in the list to production ready status.

        Returns
        -------
        Deployment
            Deployment object of the feature list. The created deployment is disabled by default.

        Examples
        --------
        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> deployment = feature_list.deploy(  # doctest: +SKIP
        ...   deployment_name="new deploy",
        ...   make_production_ready=True,
        ... )

        See Also
        --------
        - [Deployment.get_online_serving_code](/reference/featurebyte.api.deployment.Deployment.get_online_serving_code/)
        """
        self.update(
            update_payload={
                "make_production_ready": make_production_ready,
                "ignore_guardrails": ignore_guardrails,
            },
            allow_update_local=False,
        )
        deployment_payload = DeploymentCreate(name=deployment_name, feature_list_id=self.id)
        output = self.post_async_task(
            route="/deployment",
            payload=deployment_payload.json_dict(),
        )

        from featurebyte.api.deployment import Deployment  # pylint: disable=import-outside-toplevel

        return Deployment.get_by_id(ObjectId(output["_id"]))

    # descriptors
    list_versions: ClassVar[ClassInstanceMethodDescriptor] = ClassInstanceMethodDescriptor(
        class_method=_list_versions,
        instance_method=_list_versions_with_same_name,
    )

    @typechecked
    def update_namespace_description(self, description: Optional[str]) -> None:
        """
        Update description of object namespace

        Parameters
        ----------
        description: Optional[str]
            Description of the object namespace
        """
        self.feature_list_namespace.update_description(description=description)
