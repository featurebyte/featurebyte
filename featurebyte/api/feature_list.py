"""
FeatureListVersion class
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, OrderedDict, Union

import collections
import time

import pandas as pd
from alive_progress import alive_bar
from pydantic import Field, parse_obj_as, root_validator
from typeguard import typechecked

from featurebyte.api.api_object import ApiGetObject, ApiObject
from featurebyte.api.feature import Feature
from featurebyte.common.env_util import is_notebook
from featurebyte.common.model_util import get_version
from featurebyte.config import Configurations, Credentials
from featurebyte.core.mixin import ParentMixin
from featurebyte.logger import logger
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature import FeatureReadiness
from featurebyte.models.feature_list import (
    FeatureListModel,
    FeatureListNamespaceModel,
    FeatureListStatus,
)
from featurebyte.query_graph.feature_historical import get_historical_features
from featurebyte.query_graph.feature_preview import get_feature_preview_sql
from featurebyte.schema.feature_list import FeatureListCreate


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
    def feature_names(self) -> list[str]:
        """
        List of feature names

        Returns
        -------
        list[str]
        """
        return list(self.feature_objects)

    @root_validator()
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
        credentials: Optional[Credentials] = None,
    ) -> pd.DataFrame:
        """
        Preview a FeatureGroup

        Parameters
        ----------
        point_in_time_and_serving_name : Dict[str, Any]
            Dictionary consisting the point in time and serving names based on which the feature
            preview will be computed
        credentials: Optional[Credentials]
            credentials to create a database session

        Returns
        -------
        pd.DataFrame

        Raises
        ------
        ValueError
            When the FeatureGroup object is empty
        """

        tic = time.time()
        nodes = [feature.node for feature in self.feature_objects.values()]
        if nodes:
            first_feature = next(iter(self.feature_objects.values()))
            preview_sql = get_feature_preview_sql(
                graph=first_feature.graph,
                nodes=nodes,
                point_in_time_and_serving_name=point_in_time_and_serving_name,
            )
            session = first_feature.get_session(credentials)
            result = session.execute_query(preview_sql)
            elapsed = time.time() - tic
            logger.debug(f"Preview took {elapsed:.2f}s")
            return result
        raise ValueError("There is no feature in the FeatureGroup object.")


class FeatureListGetObject(ApiGetObject):
    """
    FeatureListGetObject class
    """

    @classmethod
    def _get_info_to_request_func(cls, response_dict: dict[str, Any], page: int) -> bool:
        return any(
            [
                cls._default_to_request_func(response_dict["entities"], page),
                cls._default_to_request_func(response_dict["event_data"], page),
            ]
        )

    @classmethod
    def _get_info_reduce_func(
        cls, accumulator: dict[str, Any], response_dict: dict[str, Any]
    ) -> dict[str, Any]:
        if accumulator:
            accumulator["entities"] = cls._pagination_response_reduce_func(
                accumulator["entities"], response_dict
            )
            accumulator["event_data"] = cls._pagination_response_reduce_func(
                accumulator["event_data"], response_dict
            )
        else:
            accumulator = response_dict.copy()
            accumulator["entities"] = response_dict["entities"]["data"]
            accumulator["event_data"] = response_dict["event_data"]["data"]
        return accumulator


class FeatureListNamespace(FeatureListNamespaceModel, FeatureListGetObject):
    """
    FeatureListNamespace class
    """

    # class variable
    _route = "/feature_list_namespace"


class FeatureList(BaseFeatureGroup, FeatureListModel, FeatureListGetObject, ApiObject):
    """
    FeatureList class

    items : list[Union[Feature, BaseFeatureGroup]]
        List of feature like objects to be used to create the FeatureList
    name : str
        Name of the FeatureList
    """

    # class variables
    _route = "/feature_list"

    def _get_init_params_from_object(self) -> dict[str, Any]:
        return {"items": self.items}

    @classmethod
    def _get_init_params(cls) -> dict[str, Any]:
        return {"items": []}

    def _get_create_payload(self) -> dict[str, Any]:
        data = FeatureListCreate(**self.json_dict(exclude_none=True))
        return data.json_dict()

    def _pre_save_operations(self) -> None:
        if is_notebook():
            other_kwargs = {"force_tty": True}
        else:
            other_kwargs = {"dual_line": True}

        with alive_bar(
            total=len(self.feature_objects), title="Saving Feature(s)", **other_kwargs
        ) as progress_bar:
            for feature in self.feature_objects.values():
                text = f'Feature "{feature.name}" has been saved before.'
                if not feature.saved:
                    feature.save()
                    text = f'Feature "{feature.name}" is saved.'

                # update progress bar
                progress_bar.text = text
                progress_bar()  # pylint: disable=not-callable

    @root_validator(pre=True)
    @classmethod
    def _initialize_feature_objects_and_items(cls, values: dict[str, Any]) -> dict[str, Any]:
        if "feature_ids" in values:
            if "feature_objects" not in values or "items" not in values:
                items = []
                feature_objects = collections.OrderedDict()
                for feature_id in values["feature_ids"]:
                    feature = Feature.get_by_id(feature_id)
                    items.append(feature)
                    feature_objects[feature.name] = feature
                values["items"] = items
                values["feature_objects"] = feature_objects
        return values

    @root_validator()
    @classmethod
    def _initialize_feature_list_parameters(cls, values: dict[str, Any]) -> dict[str, Any]:
        values["readiness"] = min(
            values["feature_objects"].values(),
            key=lambda feature: FeatureReadiness(feature.readiness or FeatureReadiness.min()),
        ).readiness
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

    @classmethod
    def list(cls) -> List[str]:
        return FeatureListNamespace.list()

    def _get_verbose_info(self, info_dict: dict[str, Any]) -> dict[str, Any]:
        feature_info = []
        params = {"feature_list_id": str(self.id)}
        for response_dict in self._iterate_paginated_routes(route="/feature", params=params):
            for item in response_dict["data"]:
                feature_info.append(
                    {
                        "name": item["name"],
                        "type": item["dtype"],
                        "version": item["version"],
                        "creation_date": item["created_at"],
                        "readiness": item["readiness"],
                    }
                )
        output = info_dict.copy()
        output["feature_info"] = feature_info
        return output

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
        features: list[Feature] = list(self.feature_objects.values())
        return get_historical_features(
            features,
            training_events,
            credentials=credentials,
            serving_names_mapping=serving_names_mapping,
        )
