"""
FeatureListVersion class
"""
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, OrderedDict, Union

import collections
import time
from collections import defaultdict
from http import HTTPStatus

import pandas as pd
from alive_progress import alive_bar
from pydantic import Field, parse_obj_as, root_validator
from typeguard import typechecked

from featurebyte.api.api_object import ApiGetObject, ApiObject, ConflictResolution
from featurebyte.api.feature import Feature
from featurebyte.common.env_util import get_alive_bar_additional_params
from featurebyte.common.model_util import get_version
from featurebyte.config import Configurations, Credentials
from featurebyte.core.mixin import ParentMixin
from featurebyte.core.utils import run_async
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordCreationException,
    RecordRetrievalException,
)
from featurebyte.logger import logger
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId, VersionIdentifier
from featurebyte.models.feature_list import (
    FeatureListModel,
    FeatureListNamespaceModel,
    FeatureListNewVersionMode,
    FeatureListStatus,
)
from featurebyte.query_graph.pruning_util import get_prune_graph_and_nodes
from featurebyte.query_graph.sql.feature_historical import (
    get_historical_features,
    get_historical_features_sql,
    validate_historical_requests_point_in_time,
    validate_request_schema,
)
from featurebyte.schema.feature_list import (
    FeatureListCreate,
    FeatureListPreview,
    FeatureListPreviewGroup,
    FeatureVersionInfo,
)
from featurebyte.service.preview import PreviewService
from featurebyte.utils.credential import get_credential


class BaseFeatureGroup(FeatureByteBaseModel):
    """
    BaseFeatureGroup class
    items : list[Union[Feature, BaseFeatureGroup]]
        List of feature like objects to be used to create the FeatureList
    feature_objects: OrderedDict[str, Feature]
        Dictionary of feature name to feature object
    """

    items: List[Union[Feature, BaseFeatureGroup]] = Field(exclude=True)
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
    def __init__(self, items: List[Union[Feature, BaseFeatureGroup]], **kwargs: Any):
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


class FeatureGroup(BaseFeatureGroup, ParentMixin):
    """
    FeatureGroup class
    """

    @typechecked
    def __getitem__(self, item: Union[str, List[str]]) -> Union[Feature, FeatureGroup]:
        # Note: Feature can only modify FeatureGroup parent but not FeatureList parent.
        output = super().__getitem__(item)
        if isinstance(output, Feature):
            output.set_parent(self)
        return output

    @typechecked
    def __setitem__(self, key: str, value: Feature) -> None:
        # Note: since parse_obj_as() makes a copy, the changes below don't apply to the original
        # Feature object
        value = parse_obj_as(Feature, value)
        # Name setting performs validation to ensure the specified name is valid
        value.name = key
        self.feature_objects[key] = value
        # sanity check: make sure we don't copy global query graph
        assert id(self.feature_objects[key].graph.nodes) == id(value.graph.nodes)

    @typechecked
    def preview(
        self,
        point_in_time_and_serving_name: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Preview a FeatureGroup

        Parameters
        ----------
        point_in_time_and_serving_name : Dict[str, Any]
            Dictionary consisting the point in time and serving names based on which the feature
            preview will be computed

        Returns
        -------
        pd.DataFrame

        Raises
        ------
        RecordRetrievalException
            Preview request failed
        """
        tic = time.time()

        # split features into groups that share the same feature store
        groups = defaultdict(list)
        for feature in self._features:
            groups[feature.feature_store.name].append(feature)

        # create preview group for each group
        preview_groups = []
        for feature_store_name, features in groups.items():
            pruned_graph, mapped_nodes = get_prune_graph_and_nodes(feature_objects=features)
            preview_groups.append(
                FeatureListPreviewGroup(
                    feature_store_name=feature_store_name,
                    graph=pruned_graph,
                    node_names=[node.name for node in mapped_nodes],
                )
            )

        payload = FeatureListPreview(
            preview_groups=preview_groups,
            point_in_time_and_serving_name=point_in_time_and_serving_name,
        )

        if self._features[0].feature_store.details.is_local_source:
            result = PreviewService(user=None, persistent=None).preview_featurelist(
                featurelist_preview=payload, get_credential=get_credential
            )
        else:
            client = Configurations().get_client()
            response = client.post(url="/feature_list/preview", json=payload.json_dict())
            if response.status_code != HTTPStatus.OK:
                raise RecordRetrievalException(response)
            result = response.json()

        elapsed = time.time() - tic
        logger.debug(f"Preview took {elapsed:.2f}s")
        return pd.read_json(result, orient="table", convert_dates=False)


class FeatureListNamespace(FeatureListNamespaceModel, ApiGetObject):
    """
    FeatureListNamespace class
    """

    # class variable
    _route = "/feature_list_namespace"


class FeatureList(BaseFeatureGroup, FeatureListModel, ApiObject):
    """
    FeatureList class

    items : list[Union[Feature, BaseFeatureGroup]]
        List of feature like objects to be used to create the FeatureList
    name : str
        Name of the FeatureList
    """

    # override FeatureListModel attributes
    feature_ids: List[PydanticObjectId] = Field(default_factory=list, allow_mutation=False)
    version: VersionIdentifier = Field(allow_mutation=False, default=None)

    # class variables
    _route = "/feature_list"

    def _get_init_params_from_object(self) -> dict[str, Any]:
        return {"items": self.items}

    @classmethod
    def _get_init_params(cls) -> dict[str, Any]:
        return {"items": []}

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
            with alive_bar(
                total=len(values["feature_ids"]),
                title="Loading Feature(s)",
                **get_alive_bar_additional_params(),
            ) as progress_bar:
                for feature_dict in cls._iterate_api_object_using_paginated_routes(
                    route="/feature", params={"feature_list_id": id_value, "page_size": 100}
                ):
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
        if not values.get("readiness_distribution"):
            values["readiness_distribution"] = cls.derive_readiness_distribution(features)
        return values

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
    def status(self) -> FeatureListStatus:
        """
        Retrieve feature list status at persistent

        Returns
        -------
        Feature list status
        """
        return self.feature_list_namespace.status

    @property
    def production_ready_fraction(self) -> float:
        """
        Retrieve fraction of production ready features in the feature list

        Returns
        -------
        Fraction of production ready feature
        """
        return self.readiness_distribution.derive_production_ready_fraction()

    @classmethod
    def list(cls) -> List[str]:
        return FeatureListNamespace.list()

    @typechecked
    def get_historical_features_sql(
        self,
        training_events: pd.DataFrame,
        serving_names_mapping: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Retrieve partial SQL statements used to retrieved historical features (for debugging / understanding purposes)

        Parameters
        ----------
        training_events : pd.DataFrame
            Training events DataFrame
        serving_names_mapping : Optional[Dict[str, str]]
            Optional serving names mapping if the training events data has different serving name
            columns than those defined in Entities. Mapping from original serving name to new
            serving name.

        Returns
        -------
        str
        """
        # Validate request
        validate_request_schema(training_events)
        training_events = validate_historical_requests_point_in_time(training_events)
        return get_historical_features_sql(
            feature_objects=self._features,
            request_table_columns=training_events.columns.tolist(),
            serving_names_mapping=serving_names_mapping,
        )

    @typechecked
    def get_historical_features(
        self,
        training_events: pd.DataFrame,
        credentials: Optional[Credentials] = None,
        serving_names_mapping: Optional[Dict[str, str]] = None,
    ) -> Optional[pd.DataFrame]:
        """Get historical features

        Parameters
        ----------
        training_events : pd.DataFrame
            Training events DataFrame
        credentials : Optional[Credentials]
            Optional feature store to credential mapping
        serving_names_mapping : Optional[Dict[str, str]]
            Optional serving names mapping if the training events data has different serving name
            columns than those defined in Entities. Mapping from original serving name to new
            serving name.

        Returns
        -------
        pd.DataFrame
        """
        if credentials is None:
            credentials = Configurations().credentials
        return run_async(
            get_historical_features,
            feature_objects=self._features,
            training_events=training_events,
            credentials=credentials,
            serving_names_mapping=serving_names_mapping,
        )

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
        mode: FeatureListNewVersionMode
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
