"""
FeatureListVersion class
"""
# pylint: disable=too-many-lines
from __future__ import annotations

from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Literal,
    Optional,
    OrderedDict,
    Sequence,
    Tuple,
    Union,
    cast,
)

import collections
import json
import os.path
import time
from http import HTTPStatus

import numpy as np
import pandas as pd
from alive_progress import alive_bar
from bson.objectid import ObjectId
from jinja2 import Template
from pydantic import Field, parse_obj_as, root_validator
from typeguard import typechecked

from featurebyte.api.api_object import (
    PAGINATED_CALL_PAGE_SIZE,
    ApiObject,
    ConflictResolution,
    ForeignKeyMapping,
    SavableApiObject,
)
from featurebyte.api.base_table import TableApiObject
from featurebyte.api.entity import Entity
from featurebyte.api.feature import Feature
from featurebyte.api.feature_job import FeatureJobMixin
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.table import Table
from featurebyte.common.descriptor import ClassInstanceMethodDescriptor
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.env_util import get_alive_bar_additional_params
from featurebyte.common.model_util import get_version
from featurebyte.common.typing import Scalar
from featurebyte.common.utils import (
    CodeStr,
    dataframe_from_arrow_stream,
    dataframe_from_json,
    dataframe_to_arrow_bytes,
    enforce_observation_set_row_order,
)
from featurebyte.config import Configurations
from featurebyte.core.mixin import ParentMixin
from featurebyte.core.series import Series
from featurebyte.exception import (
    DuplicatedRecordException,
    FeatureListNotOnlineEnabledError,
    RecordCreationException,
    RecordRetrievalException,
)
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.logger import logger
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId, VersionIdentifier
from featurebyte.models.feature import DefaultVersionMode, FeatureModel
from featurebyte.models.feature_list import (
    FeatureCluster,
    FeatureListModel,
    FeatureListNamespaceModel,
    FeatureListNewVersionMode,
    FeatureListStatus,
    FeatureReadinessDistribution,
    FrozenFeatureListModel,
    FrozenFeatureListNamespaceModel,
)
from featurebyte.models.relationship_analysis import derive_primary_entity
from featurebyte.models.tile import TileSpec
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.schema.feature_list import (
    FeatureListCreate,
    FeatureListGetHistoricalFeatures,
    FeatureListPreview,
    FeatureListSQL,
    FeatureListUpdate,
    FeatureVersionInfo,
)
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceUpdate


class BaseFeatureGroup(FeatureByteBaseModel):
    """
    BaseFeatureGroup class

    This class represents a collection of Feature's that users create.

    Parameters
    ----------
    items: Sequence[Union[Feature, BaseFeatureGroup]]
        List of feature like objects to be used to create the FeatureList
    feature_objects: OrderedDict[str, Feature]
        Dictionary of feature name to feature object
    """

    items: Sequence[Union[Feature, BaseFeatureGroup]] = Field(exclude=True)
    feature_objects: OrderedDict[str, Feature] = Field(
        exclude=True, default_factory=collections.OrderedDict
    )

    @property
    def _features(self) -> list[Feature]:
        """
        Retrieve list of features in the FeatureGroup object

        Returns
        -------
        List[Feature]


        Raises
        ------
        ValueError
            When the FeatureGroup object is empty
        """
        features: list[Feature] = list(self.feature_objects.values())
        if features:
            return features
        raise ValueError("There is no feature in the FeatureGroup object.")

    @property
    def feature_names(self) -> list[str]:
        """
        List of feature names

        Returns
        -------
        list[str]
        """
        return list(self.feature_objects)

    @property
    def primary_entity(self) -> List[Entity]:
        """
        Return the primary entity of the feature list or feature group.

        The main focus of a feature list is determined by its primary entity, which typically
        corresponds to the primary entity of the Use Case that the feature list was created for.

        Returns
        -------
        List[Entity]
            Primary entity
        """
        entity_ids = set()
        for feature in self._features:
            entity_ids.update(feature.entity_ids)
        primary_entity = derive_primary_entity(
            [Entity.get_by_id(entity_id) for entity_id in entity_ids]
        )
        return primary_entity

    @root_validator
    @classmethod
    def _set_feature_objects(cls, values: dict[str, Any]) -> dict[str, Any]:
        feature_objects = collections.OrderedDict()
        feature_ids = set()
        items = values.get("items", [])
        for item in items:
            if isinstance(item, Feature):
                if item.name is None:
                    raise ValueError(f'Feature (feature.id: "{item.id}") name must not be None!')
                if item.name in feature_objects:
                    raise ValueError(f'Duplicated feature name (feature.name: "{item.name}")!')
                if item.id in feature_ids:
                    raise ValueError(f'Duplicated feature id (feature.id: "{item.id}")!')
                feature_objects[item.name] = item
                feature_ids.add(item.id)
            else:
                for name, feature in item.feature_objects.items():
                    if feature.name in feature_objects:
                        raise ValueError(
                            f'Duplicated feature name (feature.name: "{feature.name}")!'
                        )
                    if feature.id in feature_ids:
                        raise ValueError(f'Duplicated feature id (feature.id: "{feature.id}")!')
                    feature_objects[name] = feature
        values["feature_objects"] = feature_objects
        return values

    @typechecked
    def __init__(self, items: Sequence[Union[Feature, BaseFeatureGroup]], **kwargs: Any):
        super().__init__(items=items, **kwargs)
        # sanity check: make sure we don't make a copy on global query graph
        for item_origin, item in zip(items, self.items):
            if isinstance(item_origin, Feature) and isinstance(item, Feature):
                assert id(item_origin.graph.nodes) == id(item.graph.nodes)

    def _subset_single_column(self, column: str) -> Feature:
        return self.feature_objects[column]

    def _subset_list_of_columns(self, columns: list[str]) -> FeatureGroup:
        return FeatureGroup([self.feature_objects[elem] for elem in columns])

    @typechecked
    def __getitem__(self, item: Union[str, List[str]]) -> Union[Feature, FeatureGroup]:
        if isinstance(item, str):
            return self._subset_single_column(item)
        return self._subset_list_of_columns(item)

    @typechecked
    def drop(self, items: List[str]) -> FeatureGroup:
        """
        Drop feature(s) from the FeatureGroup/FeatureList

        Parameters
        ----------
        items: List[str]
            List of feature names to be dropped

        Returns
        -------
        FeatureGroup
            FeatureGroup object contains remaining feature(s)
        """
        selected_feat_names = [
            feat_name for feat_name in self.feature_objects if feat_name not in items
        ]
        return self._subset_list_of_columns(selected_feat_names)

    def _get_feature_clusters(self) -> List[FeatureCluster]:
        """
        Get groups of features in the feature lists that belong to the same feature store

        Returns
        -------
        List[FeatureCluster]
        """
        return cast(
            List[FeatureCluster],
            FeatureList.derive_feature_clusters(cast(List[FeatureModel], self._features)),
        )

    @enforce_observation_set_row_order
    @typechecked
    def preview(
        self,
        observation_set: pd.DataFrame,
    ) -> Optional[pd.DataFrame]:
        """
        Materialize feature group using a small observation set of up to 50 rows.

        Unlike get_historical_features, this method does not store partial aggregations (tiles) to
        speed up future computation. Instead, it computes the features on the fly, and should be used
        only for small observation sets for debugging or prototyping unsaved features.

        Tiles are a method of storing partial aggregations in the feature store,
        which helps to minimize the resources required to fulfill historical, batch and online requests.

        Parameters
        ----------
        observation_set : pd.DataFrame
            Observation set DataFrame, which should contain the `POINT_IN_TIME` column,
            as well as columns with serving names for all entities used by features in the feature group.

        Returns
        -------
        pd.DataFrame
            Materialized feature values.
            The returned DataFrame will have the same number of rows, and include all columns from the observation set.

            **Note**: `POINT_IN_TIME` values will be converted to UTC time.

        Raises
        ------
        RecordRetrievalException
            Preview request failed

        Examples
        --------
        Create a feature group with two features.
        >>> features = fb.FeatureGroup([
        ...     catalog.get_feature("InvoiceCount_60days"),
        ...     catalog.get_feature("InvoiceAmountAvg_60days"),
        ... ])

        Prepare observation set with POINT_IN_TIME and serving names columns.
        >>> observation_set = pd.DataFrame({
        ...     "POINT_IN_TIME": ["2022-06-01 00:00:00", "2022-06-02 00:00:00"],
        ...     "GROCERYCUSTOMERGUID": [
        ...         "a2828c3b-036c-4e2e-9bd6-30c9ee9a20e3",
        ...         "ac479f28-e0ff-41a4-8e60-8678e670e80b",
        ...     ],
        ... })

        Preview the feature group with a small observation set.
        >>> features.preview(observation_set)
          POINT_IN_TIME                   GROCERYCUSTOMERGUID  InvoiceCount_60days  InvoiceAmountAvg_60days
        0    2022-06-01  a2828c3b-036c-4e2e-9bd6-30c9ee9a20e3                 10.0                    7.938
        1    2022-06-02  ac479f28-e0ff-41a4-8e60-8678e670e80b                  6.0                    9.870

        See Also
        --------
        - [Feature.preview](/reference/featurebyte.api.feature.Feature.preview/):
          Preview feature group.
        - [FeatureList.get_historical_features](/reference/featurebyte.api.feature_list.FeatureList.get_historical_features/):
          Get historical features from a feature list.
        """
        tic = time.time()

        payload = FeatureListPreview(
            feature_clusters=self._get_feature_clusters(),
            point_in_time_and_serving_name_list=observation_set.to_dict(orient="records"),
        )

        client = Configurations().get_client()
        response = client.post("/feature_list/preview", json=payload.json_dict())
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
        result = response.json()

        elapsed = time.time() - tic
        logger.debug(f"Preview took {elapsed:.2f}s")
        return dataframe_from_json(result)  # pylint: disable=no-member

    @property
    def sql(self) -> str:
        """
        Get FeatureGroup SQL

        Returns
        -------
        str
            FeatureGroup SQL

        Raises
        ------
        RecordRetrievalException
            Failed to get feature list SQL
        """
        payload = FeatureListSQL(
            feature_clusters=self._get_feature_clusters(),
        )

        client = Configurations().get_client()
        response = client.post("/feature_list/sql", json=payload.json_dict())
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)

        return cast(
            str,
            response.json(),
        )


class FeatureGroup(BaseFeatureGroup, ParentMixin):
    """
    FeatureGroup represents a collection of Feature's.

    These Features are typically not production ready, and are mostly used as an in-memory representation while
    users are still building up their features in the SDK. Note that while this object has a `save` function, it is
    actually the individual features within this feature group that get persisted. Similarly, the object that
    gets constructed on the read path does not become a FeatureGroup. The persisted version that users interact with
    is called a FeatureList.
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["FeatureGroup"], proxy_class="featurebyte.FeatureGroup")

    @typechecked
    def __getitem__(self, item: Union[str, List[str]]) -> Union[Feature, FeatureGroup]:
        # Note: Feature can only modify FeatureGroup parent but not FeatureList parent.
        output = super().__getitem__(item)
        if isinstance(output, Feature):
            output.set_parent(self)
        return output

    @typechecked
    def __setitem__(
        self, key: Union[str, Tuple[Feature, str]], value: Union[Feature, Union[Scalar, Series]]
    ) -> None:
        if isinstance(key, tuple):
            if len(key) != 2:
                raise ValueError(f"{len(key)} elements found, when we only expect 2.")
            mask = key[0]
            if not isinstance(mask, Feature):
                raise ValueError("The mask provided should be a Feature.")
            column: str = key[1]
            feature = self[column]
            assert isinstance(feature, Series)
            feature[mask] = value
            return

        # Note: since parse_obj_as() makes a copy, the changes below don't apply to the original
        # Feature object
        value = parse_obj_as(Feature, value)
        # Name setting performs validation to ensure the specified name is valid
        value.name = key
        self.feature_objects[key] = value
        # sanity check: make sure we don't copy global query graph
        assert id(self.feature_objects[key].graph.nodes) == id(value.graph.nodes)

    @typechecked
    def save(self, conflict_resolution: ConflictResolution = "raise") -> None:
        """
        Save features within a FeatureGroup object to the persistent. Conflict could be triggered when the feature
        being saved has violated uniqueness check at the persistent (for example, same ID has been used by another
        record stored at the persistent).

        Parameters
        ----------
        conflict_resolution: ConflictResolution
            "raise" raises error when then counters conflict error (default)
            "retrieve" handle conflict error by retrieving the object with the same name
        """
        for feature_name in self.feature_names:
            self[feature_name].save(conflict_resolution=conflict_resolution)


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
        "num_features",
        "status",
        "deployed",
        "readiness_frac",
        "online_frac",
        "table",
        "entities",
        "created_at",
    ]
    _list_foreign_keys = [
        ForeignKeyMapping("entity_ids", Entity, "entities"),
        ForeignKeyMapping("tabular_data_ids", TableApiObject, "table"),
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

        feature_lists["num_features"] = feature_lists.feature_namespace_ids.apply(len)
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
                feature_lists.table.apply(lambda table_list: table in table_list)
            ]
        return feature_lists


class FeatureList(BaseFeatureGroup, FrozenFeatureListModel, SavableApiObject, FeatureJobMixin):
    """
    FeatureList represents the persisted version of a collection of a features.

    The FeatureList is typically how a user interacts with their collection of features.

    items : list[Union[Feature, BaseFeatureGroup]]
        List of feature like objects to be used to create the FeatureList
    name : str
        Name of the FeatureList
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["FeatureList"],
        proxy_class="featurebyte.FeatureList",
    )

    # override FeatureListModel attributes
    feature_ids: List[PydanticObjectId] = Field(default_factory=list, allow_mutation=False)
    version: VersionIdentifier = Field(
        allow_mutation=False, default=None, description="Feature list version"
    )

    # class variables
    _route = "/feature_list"
    _update_schema_class = FeatureListUpdate
    _list_schema = FeatureListModel
    _get_schema = FeatureListModel
    _list_fields = [
        "name",
        "feature_list_namespace_id",
        "num_features",
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

    @classmethod
    def _get_init_params(cls) -> dict[str, Any]:
        return {"items": []}

    @classmethod
    def get(cls, name: str, version: Optional[str] = None) -> FeatureList:
        """
        Retrieve the FeatureList from the persistent data store given the object's name, and version.

        This assumes that the object has been saved to the persistent data store. If the object has not been saved,
        an exception will be raised and you should create and save the object first.

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
        try:
            super().save(conflict_resolution=conflict_resolution)
        except DuplicatedRecordException as exc:
            if conflict_resolution == "raise":
                raise DuplicatedRecordException(
                    exc.response,
                    resolution=' Or try `feature_list.save(conflict_resolution = "retrieve")` to resolve conflict.',
                ) from exc
            raise exc

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
                for feature_dict in cls._iterate_api_object_using_paginated_routes(
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

    @root_validator()
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
        feature_lists["num_features"] = feature_lists.feature_ids.apply(len)
        feature_lists["online_frac"] = (
            feature_lists.online_enabled_feature_ids.apply(len) / feature_lists["num_features"]
        )
        return feature_lists

    @classmethod
    def _list_versions(cls, include_id: Optional[bool] = False) -> pd.DataFrame:
        """
        List saved feature list versions

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list

        Returns
        -------
        pd.DataFrame
            Table of feature lists

        Examples
        --------

        List saved FeatureList versions

        >>> FeatureList.list_versions()
              name          feature_list_namespace_id  num_features  online_frac  deployed              created_at
        0      new_flist_2  641bfaf628dba368bde43d60             2          0.0     False   2023-03-23 07:08:38.254
        1      new_flist_1  641bfaf628dba368bde43d5e             2          0.0     False   2023-03-23 07:08:38.163
        2  my_feature_list  641bfaf528dba368bde43d4f             1          0.0     False   2023-03-23 07:08:37.877

        List FeatureList versions with the same name

        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> feature_list.list_versions()
                           name feature_list_namespace_id  num_features  online_frac  deployed              created_at
        0  invoice_feature_list  641d02af94ede33779acc6c8             1          0.0     False 2023-03-24 01:53:51.515
        """
        return super().list(include_id=include_id)

    def _list_versions_with_same_name(self, include_id: bool = False) -> pd.DataFrame:
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
        >>> feature_list.list_versions()
                           name feature_list_namespace_id  num_features  online_frac  deployed              created_at
        0  invoice_feature_list  641d02af94ede33779acc6c8             1          0.0     False 2023-03-24 01:53:51.515
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

    def list_features(
        self, entity: Optional[str] = None, table: Optional[str] = None
    ) -> pd.DataFrame:
        """
        List features in the feature list

        Parameters
        ----------
        entity: Optional[str]
            Name of entity used to filter results
        table: Optional[str]
            Name of table used to filter results

        Returns
        -------
        pd.DataFrame
            Table of features
        """
        return Feature.list_versions(feature_list_id=self.id, entity=entity, table=table)

    @typechecked
    def get_historical_features_sql(
        self,
        observation_set: pd.DataFrame,
        serving_names_mapping: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Retrieve partial SQL statements used to retrieved historical features (for debugging / understanding purposes)

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

        Raises
        ------
        RecordRetrievalException
            Get historical features request failed
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
    def get_historical_features(
        self,
        observation_set: pd.DataFrame,
        serving_names_mapping: Optional[Dict[str, str]] = None,
        max_batch_size: int = 5000,
    ) -> Optional[pd.DataFrame]:
        """
        Materialize feature list using an observation set.

        This method will store partial aggregations (tiles) to speed up future computation, and may take a
        longer time to complete the first time it is called. Subsequent calls will be faster.

        Tiles are a method of storing partial aggregations in the feature store,
        which helps to minimize the resources required to fulfill historical, batch and online requests.

        Parameters
        ----------
        observation_set : pd.DataFrame
            Observation set DataFrame, which should contain the `POINT_IN_TIME` column,
            as well as columns with serving names for all entities used by features in the feature list.
        serving_names_mapping : Optional[Dict[str, str]]
            Optional serving names mapping if the training events table has different serving name
            columns than those defined in Entities. Mapping from original serving name to new
            serving name.
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
            Get historical features request failed

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
        >>> feature_list.get_historical_features(observation_set)
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
        - [FeatureGroup.preview](/reference/featurebyte.api.feature_list.FeatureGroup.preview/):
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
    def create_new_version(
        self,
        mode: Literal[tuple(FeatureListNewVersionMode)],  # type: ignore[misc]
        features: Optional[List[FeatureVersionInfo]] = None,
    ) -> FeatureList:
        """
        Create new feature list version

        Parameters
        ----------
        mode: Literal[tuple(FeatureListNewVersionMode)]
            Feature list default version mode
        features: Optional[List[FeatureVersionInfo]]
            Specified feature version in feature list

        Returns
        -------
        FeatureList

        Raises
        ------
        RecordCreationException
            When failed to save a new version

        Examples
        --------

        Create new version of feature list with auto mode. Parameter `features` has no effect if `mode` is `auto`.

        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> feature_list.create_new_version(mode="auto")  # doctest: +SKIP

        Create new version of feature list with manual mode (only the versions of the features that are specified are
        changed). The versions of other features are the same as the origin feature list version.

        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> feature_list.create_new_version(
        ...   mode="manual",
        ...   features=[
        ...     # list of features to update, other features are the same as the original version
        ...     FeatureVersionInfo(name="InvoiceCount_60days", version="V230323_1"),
        ...   ]
        ... )


        Create new version of feature list with semi-auto mode (uses the current default versions of features except
        for the features versions that are specified).

        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> feature_list.create_new_version(
        ...   mode="auto",
        ...   features=[
        ...     # list of features to update, other features use the current default versions
        ...     FeatureVersionInfo(name="InvoiceCount_60days", version="V230323_2"),
        ...   ]
        ... )

        """
        client = Configurations().get_client()
        response = client.post(
            url=self._route,
            json={
                "source_feature_list_id": str(self.id),
                "mode": mode,
                "features": [feature.dict() for feature in features] if features else None,
            },
        )
        if response.status_code != HTTPStatus.CREATED:
            raise RecordCreationException(response=response)
        return FeatureList(**response.json(), **self._get_init_params(), saved=True)

    @typechecked
    def update_status(
        self, status: Literal[tuple(FeatureListStatus)]  # type: ignore[misc]
    ) -> None:
        """
        Update feature list status

        Parameters
        ----------
        status: Literal[tuple(FeatureListStatus)]
            Feature list status

        Examples
        --------
        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> feature_list.update_status(FeatureListStatus.PUBLISHED)
        """
        self.feature_list_namespace.update(
            update_payload={"status": str(status)}, allow_update_local=False
        )

    @typechecked
    def update_default_version_mode(
        self, default_version_mode: Literal[tuple(DefaultVersionMode)]  # type: ignore[misc]
    ) -> None:
        """
        Update feature list default version mode

        Parameters
        ----------
        default_version_mode: Literal[tuple(DefaultVersionMode)]
            Feature list default version mode

        Examples
        --------
        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> feature_list.update_default_version_mode(DefaultVersionMode.MANUAL)
        """
        self.feature_list_namespace.update(
            update_payload={"default_version_mode": DefaultVersionMode(default_version_mode).value},
            allow_update_local=False,
        )

    def as_default_version(self) -> None:
        """
        Set the feature list as the default version

        Examples
        --------
        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> feature_list.update_default_version_mode(DefaultVersionMode.MANUAL)
        >>> feature_list.as_default_version()
        """
        self.feature_list_namespace.update(
            update_payload={"default_feature_list_id": self.id}, allow_update_local=False
        )
        assert self.feature_list_namespace.default_feature_list_id == self.id

    @typechecked
    def deploy(
        self, enable: bool, make_production_ready: bool = False, ignore_guardrails: bool = False
    ) -> None:
        """
        Deploy all the Features in the Feature List to the Feature Store and update its deployment status

        Parameters
        ----------
        enable: bool
            Whether to deploy this feature list
        make_production_ready: bool
            Whether to convert the feature to production ready if it is not production ready
        ignore_guardrails: bool
            Whether to ignore guardrails when trying to promote features in the list to production ready status

        Examples
        --------
        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> feature_list.deploy(enable=True, make_production_ready=True)  # doctest: +SKIP
        """
        self.update(
            update_payload={
                "make_production_ready": make_production_ready,
                "ignore_guardrails": ignore_guardrails,
            },
            allow_update_local=False,
        )

        self.post_async_task(
            route=f"{self._route}/{self.id}/deploy",
            payload={
                "deployed": enable,
            },
        )

    def get_online_serving_code(self, language: Literal["python", "sh"] = "python") -> str:
        """
        Retrive either Python or shell script template for serving online features from a deployed featurelist, defaulted to python.

        Parameters
        ----------
        language: Literal["python", "sh"]
            Language for which to get code template

        Returns
        -------
        str

        Raises
        ------
        FeatureListNotOnlineEnabledError
            Feature list not deployed
        NotImplementedError
            Serving code not available

        Examples
        --------

        Retrieve python code template when "language" is set to "python"

        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> feature_list.get_online_serving_code(language="python")  # doctest: +SKIP
            from typing import Any, Dict
            import pandas as pd
            import requests
            def request_features(entity_serving_names: Dict[str, Any]) -> pd.DataFrame:
                "
                Send POST request to online serving endpoint
                Parameters
                ----------
                entity_serving_names: Dict[str, Any]
                    Entity serving name values to used for serving request
                Returns
                -------
                pd.DataFrame
                "
                response = requests.post(
                    url="http://localhost:8080/feature_list/{feature_list.id}/online_features",
                    params={{"catalog_id": "63eda344d0313fb925f7883a"}},
                    headers={{"Content-Type": "application/json", "Authorization": "Bearer token"}},
                    json={{"entity_serving_names": entity_serving_names}},
                )
                assert response.status_code == 200, response.json()
                return pd.DataFrame.from_dict(response.json()["features"])
            request_features([{{"cust_id": "sample_cust_id"}}])

        Retrieve shell script template when "language" is set to "sh"

        >>> feature_list = catalog.get_feature_list("invoice_feature_list")
        >>> feature_list.get_online_serving_code(language="sh")  # doctest: +SKIP
            \\#!/bin/sh
            curl -X POST -H 'Content-Type: application/json' -H 'Authorization: Bearer token' -d \\
                '{{"entity_serving_names": [{{"cust_id": "sample_cust_id"}}]}}' \\
                http://localhost:8080/feature_list/641cf594f74f839cf9297884/online_features?catalog_id=63eda344d0313fb925f7883a
        """
        if not self.deployed:
            raise FeatureListNotOnlineEnabledError("Feature list is not deployed.")

        templates = {"python": "python.tpl", "sh": "shell.tpl"}
        template_file = templates.get(language)
        if not template_file:
            raise NotImplementedError(f"Supported languages: {list(templates.keys())}")

        # get entities and tables used for the feature list
        num_rows = 1
        info = self.info()
        entities = {
            Entity.get(entity["name"]).id: {"serving_name": entity["serving_names"]}
            for entity in info["primary_entity"]
        }
        for tabular_source in info["tables"]:
            data = Table.get(tabular_source["name"])
            entity_columns = [
                column for column in data.columns_info if column.entity_id in entities
            ]
            if entity_columns:
                sample_data = data.preview(num_rows)
                for column in entity_columns:
                    entities[column.entity_id]["sample_value"] = sample_data[column.name].to_list()

        entity_serving_names = json.dumps(
            [
                {
                    entity["serving_name"][0]: entity["sample_value"][row_idx]
                    for entity in entities.values()
                }
                for row_idx in range(num_rows)
            ]
        )

        # construct serving url
        current_profile = Configurations().profile
        assert current_profile
        serving_endpoint = info["serving_endpoint"]
        headers = {"Content-Type": "application/json"}
        if current_profile.api_token:
            headers["Authorization"] = f"Bearer {current_profile.api_token}"
        header_params = " ".join([f"-H '{key}: {value}'" for key, value in headers.items()])
        serving_url = f"{current_profile.api_url}{serving_endpoint}"

        # populate template
        with open(
            file=os.path.join(
                os.path.dirname(__file__), f"templates/online_serving/{template_file}"
            ),
            mode="r",
            encoding="utf-8",
        ) as file_object:
            template = Template(file_object.read())

        return CodeStr(
            template.render(
                catalog_id=self.catalog_id,
                headers=json.dumps(headers),
                header_params=header_params,
                serving_url=serving_url,
                entity_serving_names=entity_serving_names,
            )
        )

    # descriptors
    list_versions: ClassVar[ClassInstanceMethodDescriptor] = ClassInstanceMethodDescriptor(
        class_method=_list_versions,
        instance_method=_list_versions_with_same_name,
    )
