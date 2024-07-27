"""
Feature group module.
"""

from __future__ import annotations

import collections
import time
from datetime import datetime
from http import HTTPStatus
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    OrderedDict,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
)

import pandas as pd
from alive_progress import alive_bar
from bson import ObjectId
from pydantic import Field, TypeAdapter
from typeguard import typechecked

from featurebyte.api.api_object_util import (
    PAGINATED_CALL_PAGE_SIZE,
    delete_api_object_by_id,
    get_api_object_by_id,
    iterate_api_object_using_paginated_routes,
)
from featurebyte.api.entity import Entity
from featurebyte.api.feature import Feature
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.mixin import AsyncMixin
from featurebyte.api.observation_table import ObservationTable
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.env_util import get_alive_bar_additional_params
from featurebyte.common.utils import dataframe_from_json, enforce_observation_set_row_order
from featurebyte.config import Configurations
from featurebyte.core.mixin import ParentMixin
from featurebyte.core.series import Series
from featurebyte.enum import ConflictResolution
from featurebyte.exception import RecordRetrievalException
from featurebyte.logging import get_logger
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import FeatureCluster, FeatureListModel
from featurebyte.models.relationship_analysis import derive_primary_entity
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.schema.constant import MAX_BATCH_FEATURE_ITEM_COUNT
from featurebyte.schema.feature import BatchFeatureCreatePayload, BatchFeatureItem
from featurebyte.schema.feature_list import FeatureListCreateJob, FeatureListPreview, FeatureListSQL
from featurebyte.schema.worker.task.feature_list_create import FeatureParameters
from featurebyte.typing import Scalar

logger = get_logger(__name__)


Item = Union[Feature, "BaseFeatureGroup"]
FeatureObjects = OrderedDict[str, Feature]


class BaseFeatureGroup(AsyncMixin):
    """
    BaseFeatureGroup class

    This class represents a collection of Feature's that users create.

    Parameters
    ----------
    items: Sequence[Item]
        List of feature like objects to be used to create the FeatureList
    feature_objects: FeatureObjects
        Dictionary of feature name to feature object
    """

    items: Sequence[Item] = Field(
        exclude=True,
        description="A sequence that consists of Feature, FeatureList, and FeatureGroup objects. This sequence is used "
        "to create a new FeatureGroup that contains the Feature objects found within the provided items.",
    )
    feature_objects: FeatureObjects = Field(exclude=True, default_factory=collections.OrderedDict)

    @classmethod
    def _flatten_items(cls, items: Sequence[Item]) -> FeatureObjects:
        feature_objects = collections.OrderedDict()
        feature_ids = set()
        for item in items:
            if isinstance(item, Feature):
                if item.name is None:
                    raise ValueError(f'Feature (feature.id: "{item.id}") name must not be None!')
                if item.name in feature_objects:
                    raise ValueError(f'Duplicated feature name (feature.name: "{item.name}")!')
                if item.id in feature_ids:
                    raise ValueError(f'Duplicated feature id (feature.id: "{item.id}")!')
                feature_objects[item.name] = item.copy(deep=True)
                feature_objects[item.name].set_parent(None)
                feature_ids.add(item.id)
            else:
                for name, feature in item.feature_objects.items():
                    if feature.name in feature_objects:
                        raise ValueError(
                            f'Duplicated feature name (feature.name: "{feature.name}")!'
                        )
                    if feature.id in feature_ids:
                        raise ValueError(f'Duplicated feature id (feature.id: "{feature.id}")!')
                    feature_objects[name] = feature.copy(deep=True)
                    feature_objects[name].set_parent(None)
        return feature_objects

    @staticmethod
    def _initialize_items_and_feature_objects_from_persistent(
        feature_list_id: ObjectId, feature_ids: List[str]
    ) -> Tuple[Sequence[Item], FeatureObjects]:
        feature_id_to_object = {}
        feature_store_map: Dict[ObjectId, FeatureStore] = {}
        with alive_bar(
            total=len(feature_ids),
            title="Loading Feature(s)",
            **get_alive_bar_additional_params(),
        ) as progress_bar:
            for feature_dict in iterate_api_object_using_paginated_routes(
                route="/feature",
                params={"feature_list_id": feature_list_id, "page_size": PAGINATED_CALL_PAGE_SIZE},
            ):
                # store the feature store retrieve result to reuse it if same feature store are called again
                feature_store_id = TabularSource(**feature_dict["tabular_source"]).feature_store_id
                if feature_store_id not in feature_store_map:
                    feature_store_map[feature_store_id] = FeatureStore.get_by_id(feature_store_id)
                feature_dict["feature_store"] = feature_store_map[feature_store_id]

                # deserialize feature record into feature object
                feature = Feature.from_persistent_object_dict(object_dict=feature_dict)
                feature_id_to_object[str(feature.id)] = feature
                progress_bar.text = feature.name
                progress_bar()

            # preserve the order of features
            items = []
            feature_objects = collections.OrderedDict()
            for feature_id in feature_ids:
                feature = feature_id_to_object[str(feature_id)]
                assert feature.name is not None
                feature_objects[feature.name] = feature
                items.append(feature)
        return items, feature_objects

    @typechecked
    def __init__(self, items: Sequence[Item], **kwargs: Any):
        if items:
            # handle the case where the object is created from a list of
            # Feature / FeatureGroup / FeatureList objects
            kwargs["feature_objects"] = self._flatten_items(items)

        super().__init__(items=items, **kwargs)
        # sanity check: make sure we don't make a copy on global query graph
        for item_origin, item in zip(items, self.items):
            if isinstance(item_origin, Feature) and isinstance(item, Feature):
                assert id(item_origin.graph.nodes) == id(item.graph.nodes)

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
        List of feature names in the FeatureGroup object.

        Returns
        -------
        list[str]
            List of feature names

        Examples
        --------
        >>> table = catalog.get_table("GROCERYINVOICE")
        >>> view = table.get_view()
        >>> feature_group = view.groupby("GroceryCustomerGuid").aggregate_over(
        ...     value_column=None,
        ...     method="count",
        ...     feature_names=["count_60days", "count_90days"],
        ...     windows=["60d", "90d"],
        ... )
        >>> feature_group.feature_names
        ['count_60days', 'count_90days']

        See Also
        --------
        - [FeatureGroup](/reference/featurebyte.api.feature_group.FeatureGroup/)
        - [FeatureList.feature_names](/reference/featurebyte.api.feature_list.FeatureList.feature_names/)
        """
        return list(self.feature_objects)

    @property
    def primary_entity(self) -> List[Entity]:
        """
        Returns the primary entity of the FeatureList object.

        Returns
        -------
        List[Entity]
            Primary entity
        """
        entity_ids: Set[ObjectId] = set()
        for feature in self._features:
            entity_ids.update(feature.entity_ids)
        primary_entity = derive_primary_entity(  # type: ignore
            [Entity.get_by_id(entity_id) for entity_id in entity_ids]
        )
        return primary_entity

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
        Drops feature(s) from the original FeatureGroup and returns a new FeatureGroup object.

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
        >>> features = fb.FeatureGroup([
        ...     catalog.get_feature("InvoiceCount_60days"),
        ...     catalog.get_feature("InvoiceAmountAvg_60days"),
        ... ])
        >>> amount_feature_group = features.drop(["InvoiceCount_60days"])
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
        feature_models = [
            FeatureModel(**feature.model_dump(by_alias=True)) for feature in self._features
        ]
        return FeatureListModel.derive_feature_clusters(feature_models)

    @enforce_observation_set_row_order
    @typechecked
    def preview(
        self,
        observation_set: Union[ObservationTable, pd.DataFrame],
        serving_names_mapping: Optional[Dict[str, str]] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Materializes a FeatureGroup object using a small observation set of up to 50 rows. Unlike
        compute_historical_features, this method does not store partial aggregations (tiles) to speed up future
        computation. Instead, it computes the features on the fly, and should be used only for small observation
        sets for debugging or prototyping unsaved features.

        The small observation set should combine historical points-in-time and key values of the primary entity from
        the feature group. Associated serving entities can also be utilized.

        Parameters
        ----------
        observation_set: Union[ObservationTable, pd.DataFrame]
            Observation set with `POINT_IN_TIME` and serving names columns. This can be either an
            ObservationTable or a pandas DataFrame.
        serving_names_mapping : Optional[Dict[str, str]]
            Optional serving names mapping if the observation table has different serving name

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
        - [FeatureList.compute_historical_features](/reference/featurebyte.api.feature_list.FeatureList.compute_historical_features/):
          Get historical features from a feature list.
        """
        tic = time.time()

        preview_parameters: Dict[str, Any] = {
            "feature_clusters": self._get_feature_clusters(),
            "serving_names_mapping": serving_names_mapping,
        }
        if isinstance(observation_set, ObservationTable):
            preview_parameters["observation_table_id"] = observation_set.id
        else:
            preview_parameters["point_in_time_and_serving_name_list"] = observation_set.to_dict(
                orient="records"
            )

        payload = FeatureListPreview(**preview_parameters)
        client = Configurations().get_client()
        response = client.post("/feature_list/preview", json=payload.json_dict())
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
        result = response.json()

        elapsed = time.time() - tic
        logger.debug(f"Preview took {elapsed:.2f}s")
        return dataframe_from_json(result)

    @property
    def sql(self) -> str:
        """
        Get FeatureGroup SQL.

        Returns
        -------
        str
            FeatureGroup SQL string.

        Raises
        ------
        RecordRetrievalException
            Failed to get feature list SQL.
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

    def _get_batch_feature_create_payload(
        self,
        feature_names: List[str],
        conflict_resolution: ConflictResolution,
    ) -> Tuple[BatchFeatureCreatePayload, List[BatchFeatureItem]]:
        pruned_graph, node_name_map = GlobalQueryGraph().quick_prune(
            target_node_names=[
                self.feature_objects[feat_name].node_name for feat_name in feature_names
            ]
        )
        batch_feature_items = []
        for feat_name in feature_names:
            feat = self.feature_objects[feat_name]
            batch_feature_items.append(
                BatchFeatureItem(
                    id=feat.id,
                    name=feat.name,
                    node_name=node_name_map[feat.node_name],
                    tabular_source=feat.tabular_source,
                )
            )

        # if feature with same name already exists, retrieve it
        batch_feature_create = BatchFeatureCreatePayload(
            graph=pruned_graph,
            features=batch_feature_items,
            conflict_resolution=conflict_resolution,
        )
        return batch_feature_create, batch_feature_items

    @staticmethod
    def _get_feature_list_create_job_payload(
        feature_list_id: ObjectId,
        feature_list_name: str,
        features: List[BatchFeatureItem],
        features_conflict_resolution: ConflictResolution,
    ) -> FeatureListCreateJob:
        return FeatureListCreateJob(
            _id=feature_list_id,
            name=feature_list_name,
            features=[FeatureParameters(id=feature.id, name=feature.name) for feature in features],
            features_conflict_resolution=features_conflict_resolution,
        )

    def _save_feature_list(
        self,
        feature_list_name: str,
        feature_list_id: ObjectId,
        conflict_resolution: ConflictResolution,
    ) -> None:
        # save features in batch
        feature_names = self.feature_names
        batch_size = MAX_BATCH_FEATURE_ITEM_COUNT
        feature_items = []
        for idx in range(0, len(feature_names), batch_size):
            batch_feature_names = feature_names[idx : idx + batch_size]
            batch_feature_create, batch_feature_items = self._get_batch_feature_create_payload(
                feature_names=batch_feature_names, conflict_resolution=conflict_resolution
            )
            feature_items.extend(batch_feature_items)
            self.post_async_task(
                route="/feature/batch",
                payload=batch_feature_create.json_dict(),
                retrieve_result=False,
                has_output_url=False,
            )

        # prepare feature list batch feature create payload
        feature_list_batch_feature_create = self._get_feature_list_create_job_payload(
            feature_list_id=feature_list_id,
            feature_list_name=feature_list_name,
            features=feature_items,
            features_conflict_resolution=conflict_resolution,
        )
        self.post_async_task(
            route="/feature_list/job",
            payload=feature_list_batch_feature_create.json_dict(),
            retrieve_result=False,
            has_output_url=False,
        )


class FeatureGroup(BaseFeatureGroup, ParentMixin):
    """
    FeatureGroup class is the constructor class to create a FeatureGroup object. A FeatureGroup is a temporary
    collection of Feature objects that enables the easy management of features and creation of a feature list.

    Note that while FeatureGroup has a `save` function, it is actually the individual Feature objects within this
    feature group that get added to the catalog. To add a FeatureGroup to the catalog, you need first to convert
    it into a FeatureList object.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.FeatureGroup",
        hide_keyword_only_params_in_class_docs=True,
    )

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

        # Note: since a copy is created, the changes below don't apply to the original
        # Feature object
        feature = TypeAdapter(Feature).validate_python(value).copy(deep=True)
        assert isinstance(feature, Feature)
        # Name setting performs validation to ensure the specified name is valid
        feature.name = key
        self.feature_objects[key] = feature
        # sanity check: make sure we don't copy global query graph
        assert id(self.feature_objects[key].graph.nodes) == id(feature.graph.nodes)

    @typechecked
    def save(self, conflict_resolution: ConflictResolution = "raise") -> None:
        """
        Adds each Feature object within a FeatureGroup object to the catalog.

        A conflict could be triggered when the Feature objects being saved have violated a uniqueness check. If
        uniqueness is violated, you can either raise an error or retrieve the object with the same name, depending
        on the conflict resolution parameter passed in. The default behavior is to raise an error.

        Parameters
        ----------
        conflict_resolution: ConflictResolution
            "raise" raises error when then counters conflict error (default)
            "retrieve" handle conflict error by retrieving the object with the same name

        Examples
        --------
        >>> features = fb.FeatureGroup([
        ...     catalog.get_feature("InvoiceCount_60days"),
        ...     catalog.get_feature("InvoiceAmountAvg_60days"),
        ... ])
        >>> features.save()  # doctest: +SKIP
        """
        temp_feature_list_id = ObjectId()
        self._save_feature_list(
            feature_list_id=temp_feature_list_id,
            feature_list_name=f"_temporary_feature_list_{datetime.now()}",
            conflict_resolution=conflict_resolution,
        )

        try:
            feature_list_dict = get_api_object_by_id(
                route="/feature_list", id_value=temp_feature_list_id
            )
            items, feature_objects = self._initialize_items_and_feature_objects_from_persistent(
                feature_list_id=temp_feature_list_id,
                feature_ids=feature_list_dict["feature_ids"],
            )
            self.items = items
            self.feature_objects = feature_objects

        finally:
            # delete temporary feature list
            delete_api_object_by_id(route="/feature_list", id_value=temp_feature_list_id)
