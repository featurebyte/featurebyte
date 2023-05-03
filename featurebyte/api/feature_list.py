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

import collections
from http import HTTPStatus

import numpy as np
import pandas as pd
from alive_progress import alive_bar
from bson.objectid import ObjectId
from pydantic import Field, root_validator
from typeguard import typechecked

from featurebyte.api.api_object import (
    PAGINATED_CALL_PAGE_SIZE,
    ApiObject,
    ConflictResolution,
    DeletableApiObject,
    ForeignKeyMapping,
    SavableApiObject,
)
from featurebyte.api.base_table import TableApiObject
from featurebyte.api.entity import Entity
from featurebyte.api.feature import Feature
from featurebyte.api.feature_group import BaseFeatureGroup, FeatureGroup
from featurebyte.api.feature_job import FeatureJobMixin, FeatureJobStatusResult
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.historical_feature_table import HistoricalFeatureTable
from featurebyte.api.observation_table import ObservationTable
from featurebyte.common.descriptor import ClassInstanceMethodDescriptor
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.env_util import get_alive_bar_additional_params
from featurebyte.common.model_util import get_version
from featurebyte.common.utils import (
    dataframe_from_arrow_stream,
    dataframe_to_arrow_bytes,
    enforce_observation_set_row_order,
)
from featurebyte.config import Configurations
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordCreationException,
    RecordRetrievalException,
)
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.base import PydanticObjectId, VersionIdentifier
from featurebyte.models.feature import DefaultVersionMode
from featurebyte.models.feature_list import (
    FeatureListModel,
    FeatureListNamespaceModel,
    FeatureListStatus,
    FeatureReadinessDistribution,
    FrozenFeatureListModel,
    FrozenFeatureListNamespaceModel,
)
from featurebyte.models.tile import TileSpec
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.schema.deployment import DeploymentCreate
from featurebyte.schema.feature_list import (
    FeatureListCreate,
    FeatureListGetHistoricalFeatures,
    FeatureListUpdate,
    FeatureVersionInfo,
)
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceUpdate
from featurebyte.schema.historical_feature_table import HistoricalFeatureTableCreate

if TYPE_CHECKING:
    from featurebyte.api.deployment import Deployment
else:
    Deployment = TypeVar("Deployment")


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

    _list_schema = FeatureListNamespaceModel
    _get_schema = FeatureListNamespaceModel
    _list_fields = [
        "name",
        "num_feature",
        "status",
        "deployed",
        "readiness_frac",
        "online_frac",
        "tables",
        "entities",
        "created_at",
    ]
    _list_foreign_keys = [
        ForeignKeyMapping("entity_ids", Entity, "entities"),
        ForeignKeyMapping("table_ids", TableApiObject, "tables"),
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
        """
        return self.cached_model.status

    @classmethod
    def _post_process_list(cls, item_list: pd.DataFrame) -> pd.DataFrame:
        feature_lists = super()._post_process_list(item_list)

        # add information about default feature list version
        feature_list_versions = FeatureList.list_versions(include_id=True)
        feature_lists = feature_lists.merge(
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

    @classmethod
    def list(
        cls,
        include_id: Optional[bool] = False,
        entity: Optional[str] = None,
        table: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        List saved feature lists

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
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
class FeatureList(
    BaseFeatureGroup, FrozenFeatureListModel, DeletableApiObject, SavableApiObject, FeatureJobMixin
):
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
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        proxy_class="featurebyte.FeatureList",
        hide_keyword_only_params_in_class_docs=True,
    )

    # override FeatureListModel attributes
    feature_ids: List[PydanticObjectId] = Field(
        default_factory=list,
        allow_mutation=False,
        description="Returns the unique identifier (ID) of the Feature objects associated with the FeatureList object.",
    )
    version: VersionIdentifier = Field(
        allow_mutation=False,
        default=None,
        description="Returns the version identifier of a FeatureList object.",
    )

    # class variables
    _route = "/feature_list"
    _update_schema_class = FeatureListUpdate
    _list_schema = FeatureListModel
    _get_schema = FeatureListModel
    _list_fields = [
        "name",
        "version",
        "feature_list_namespace_id",
        "num_feature",
        "online_frac",
        "deployed",
        "created_at",
    ]

    def _get_init_params_from_object(self) -> dict[str, Any]:
        return {"items": self.items}

    def _get_feature_tiles_specs(self) -> List[Tuple[str, List[TileSpec]]]:
        feature_tile_specs = []
        for feature in self.feature_objects.values():
            tile_specs = ExtendedFeatureModel(**feature.dict()).tile_specs
            if tile_specs:
                feature_tile_specs.append((str(feature.name), tile_specs))
        return feature_tile_specs

    @typechecked
    def get_feature_jobs_status(  # pylint: disable=useless-parent-delegation
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
        """
        return super().get_feature_jobs_status(
            job_history_window=job_history_window, job_duration_tolerance=job_duration_tolerance
        )

    def info(  # pylint: disable=useless-parent-delegation
        self, verbose: bool = False
    ) -> Dict[str, Any]:
        """
        Returns a dictionary that summarizes the essential information of an FeatureList object. The dictionary
        contains the following keys:

        - `name`: The name of the FeatureList object.
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

        This method is only available for FeatureList objects that are saved in the catalog.

        Parameters
        ----------
        verbose: bool
            Control verbose level of the summary.

        Returns
        -------
        Dict[str, Any]
            Key-value mapping of properties of the object.
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
            **{**self.json_dict(exclude_none=True), "feature_ids": feature_ids}
        )
        return data.json_dict()

    def _pre_save_operations(self, conflict_resolution: ConflictResolution = "raise") -> None:
        with alive_bar(
            total=len(self.feature_objects),
            title="Saving Feature(s)",
            **get_alive_bar_additional_params(),
        ) as progress_bar:
            for feat_name in self.feature_objects:
                text = f'Feature "{feat_name}" has been saved before.'
                if not self.feature_objects[feat_name].saved:
                    self.feature_objects[feat_name].save(conflict_resolution=conflict_resolution)
                    text = f'Feature "{feat_name}" is saved.'

                # update progress bar
                progress_bar.text = text
                progress_bar()  # pylint: disable=not-callable

    def save(self, conflict_resolution: ConflictResolution = "raise") -> None:
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

        Raises
        ------
        DuplicatedRecordException
            When a record with the same key exists at the persistent data store.
        """
        try:
            super().save(conflict_resolution=conflict_resolution)
        except DuplicatedRecordException as exc:
            if conflict_resolution == "raise":
                raise DuplicatedRecordException(
                    exc.response,
                    resolution=' Or try `feature_list.save(conflict_resolution = "retrieve")` to resolve conflict.',
                ) from exc
            raise exc

    def delete(self) -> None:
        """
        Delete a FeatureList object from the persistent data store. A feature list can only be deleted if
        * the feature list status is DRAFT
        * the feature list is not a default feature list with manual version mode

        Examples
        --------
        Delete a FeatureList.

        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> feature_list.delete()  # doctest: +SKIP
        """
        self._delete()

    @typechecked
    def drop(self, items: List[str]) -> FeatureGroup:  # pylint: disable=useless-parent-delegation
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
        """
        return super().drop(items=items)

    @property
    def saved(self) -> bool:  # pylint: disable=useless-parent-delegation
        """
        Returns whether the FeatureList object is saved and added to the catalog.

        Returns
        -------
        bool
        """
        return super().saved

    @typechecked
    def preview(  # pylint: disable=useless-parent-delegation
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
        """
        return super().preview(observation_set=observation_set)

    @root_validator(pre=True)
    @classmethod
    def _initialize_feature_objects_and_items(cls, values: dict[str, Any]) -> dict[str, Any]:
        if "feature_ids" in values:
            # FeatureList object constructed in SDK will not have feature_ids attribute,
            # only the record retrieved from the persistent contains this attribute.
            # Use this check to decide whether to make API call to retrieve features.
            items = []
            feature_objects = collections.OrderedDict()
            id_value = values["_id"]
            feature_store_map: Dict[ObjectId, FeatureStore] = {}
            with alive_bar(
                total=len(values["feature_ids"]),
                title="Loading Feature(s)",
                **get_alive_bar_additional_params(),
            ) as progress_bar:
                for feature_dict in cls.iterate_api_object_using_paginated_routes(
                    route="/feature",
                    params={"feature_list_id": id_value, "page_size": PAGINATED_CALL_PAGE_SIZE},
                ):
                    # store the feature store retrieve result to reuse it if same feature store are called again
                    feature_store_id = TabularSource(
                        **feature_dict["tabular_source"]
                    ).feature_store_id
                    if feature_store_id not in feature_store_map:
                        feature_store_map[feature_store_id] = FeatureStore.get_by_id(
                            feature_store_id
                        )
                    feature_dict["feature_store"] = feature_store_map[feature_store_id]

                    # deserialize feature record into feature object
                    feature = Feature.from_persistent_object_dict(object_dict=feature_dict)
                    items.append(feature)
                    feature_objects[feature.name] = feature
                    progress_bar.text = feature.name
                    progress_bar()  # pylint: disable=not-callable

            values["items"] = items
            values["feature_objects"] = feature_objects
        return values

    @root_validator
    @classmethod
    def _initialize_feature_list_parameters(cls, values: dict[str, Any]) -> dict[str, Any]:
        # set the following values if it is empty (used mainly by the SDK constructed feature list)
        # for the feature list constructed during serialization, following codes should be skipped
        features = list(values["feature_objects"].values())
        if not values.get("feature_ids"):
            values["feature_ids"] = [feature.id for feature in features]
        if not values.get("version"):
            values["version"] = get_version()
        return values

    @typechecked
    def __init__(self, items: Sequence[Union[Feature, BaseFeatureGroup]], name: str, **kwargs: Any):
        super().__init__(items=items, name=name, **kwargs)

    @property
    def feature_list_namespace(self) -> FeatureListNamespace:
        """
        FeatureListNamespace object of current feature list

        Returns
        -------
        FeatureListNamespace
        """
        return FeatureListNamespace.get_by_id(id=self.feature_list_namespace_id)

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
            return self.derive_readiness_distribution(list(self.feature_objects.values()))  # type: ignore

    @property
    def production_ready_fraction(self) -> float:
        """
        Retrieve fraction of production ready features in the feature list

        Returns
        -------
        Fraction of production ready feature
        """
        return self.readiness_distribution.derive_production_ready_fraction()

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
    def _post_process_list(cls, item_list: pd.DataFrame) -> pd.DataFrame:
        feature_lists = super()._post_process_list(item_list)
        feature_lists["version"] = feature_lists["version"].apply(
            lambda version_dict: VersionIdentifier(**version_dict).to_str()
        )
        feature_lists["num_feature"] = feature_lists.feature_ids.apply(len)
        feature_lists["online_frac"] = (
            feature_lists.online_enabled_feature_ids.apply(len) / feature_lists["num_feature"]
        )
        return feature_lists

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

        >>> FeatureList.list_versions()  # doctest: +SKIP
                           name feature_list_namespace_id  num_feature  online_frac  deployed              created_at
        0  invoice_feature_list  641d2f94f8d79eb6fee0a335            1          0.0     False 2023-03-24 05:05:24.875

        List FeatureList versions with the same name (calling from FeatureList object):

        >>> feature_list = catalog.get_feature_list("invoice_feature_list") # doctest: +SKIP
        >>> feature_list.list_versions()  # doctest: +SKIP
                           name feature_list_namespace_id  num_feature  online_frac  deployed              created_at
        0  invoice_feature_list  641d02af94ede33779acc6c8            1          0.0     False 2023-03-24 01:53:51.515

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
        >>> feature_list.list_versions()  # doctest: +SKIP
                           name feature_list_namespace_id  num_feature  online_frac  deployed              created_at
        0  invoice_feature_list  641d02af94ede33779acc6c8            1          0.0     False 2023-03-24 01:53:51.515
        """
        return self._list(include_id=include_id, params={"name": self.name})

    @classmethod
    def list(
        cls,
        include_id: Optional[bool] = False,
        entity: Optional[str] = None,
        table: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        List saved feature lists

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        entity: Optional[str]
            Name of entity used to filter results
        table: Optional[str]
            Name of table used to filter results

        Returns
        -------
        pd.DataFrame
            Table of feature lists
        """
        return FeatureListNamespace.list(include_id=include_id, entity=entity, table=table)

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
        max_batch_size: int = 5000,
    ) -> Optional[pd.DataFrame]:
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
        max_batch_size: int
            Maximum number of rows per batch.

        Returns
        -------
        pd.DataFrame
            Materialized historical features.

            **Note**: `POINT_IN_TIME` values will be converted to UTC time.

        Raises
        ------
        RecordRetrievalException
            Get historical features request failed.

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

        See Also
        --------
        - [FeatureGroup.preview](/reference/featurebyte.api.feature_group.FeatureGroup.preview/):
          Preview feature group.
        - [Feature.preview](/reference/featurebyte.api.feature.Feature.preview/):
          Preview feature group.
        """
        payload = FeatureListGetHistoricalFeatures(
            feature_list_id=self.id,
            feature_clusters=self._get_feature_clusters(),
            serving_names_mapping=serving_names_mapping,
        )

        client = Configurations().get_client()
        output = []
        with alive_bar(
            total=int(np.ceil(len(observation_set) / max_batch_size)),
            title="Retrieving Historical Feature(s)",
            **get_alive_bar_additional_params(),
        ) as progress_bar:
            for _, batch in observation_set.groupby(
                np.arange(len(observation_set)) // max_batch_size
            ):
                response = client.post(
                    "/feature_list/historical_features",
                    data={"payload": payload.json()},
                    files={"observation_set": dataframe_to_arrow_bytes(batch)},
                )
                if response.status_code != HTTPStatus.OK:
                    raise RecordRetrievalException(
                        response,
                        resolution=(
                            f"\nIf the error is related to connection broken, "
                            f"try to use a smaller `max_batch_size` parameter (current value: {max_batch_size})."
                        ),
                    )

                output.append(dataframe_from_arrow_stream(response.content))
                progress_bar()  # pylint: disable=not-callable

        return pd.concat(output, ignore_index=True)

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
        >>> feature_list.update_status(FeatureListStatus.TEMPLATE)
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
        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> feature_list.update_default_version_mode(DefaultVersionMode.MANUAL)

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
        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> feature_list.update_default_version_mode(DefaultVersionMode.MANUAL)
        >>> feature_list.as_default_version()

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
        >>> deployment = feature_list.deploy(make_production_ready=True)  # doctest: +SKIP

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
