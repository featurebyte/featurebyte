"""
FeatureListVersion class
"""
from __future__ import annotations

from typing import Any, List, OrderedDict, Union

import collections
import time

import pandas as pd
from pydantic import BaseModel, Field, parse_obj_as, root_validator

from featurebyte.api.feature import Feature
from featurebyte.common.model_util import get_version
from featurebyte.config import Configurations, Credentials
from featurebyte.logger import logger
from featurebyte.models.feature import FeatureListModel, FeatureListStatus, FeatureReadiness
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.feature_historical import get_historical_features
from featurebyte.query_graph.feature_preview import get_feature_preview_sql
from featurebyte.query_graph.graph import GlobalQueryGraph


class BaseFeatureGroup(BaseModel):
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

    def __init__(self, items: list[Union[Feature, BaseFeatureGroup]], **kwargs: Any):
        super().__init__(items=items, **kwargs)
        # sanity check: make sure we don't make a copy on global query graph
        for item_origin, item in zip(items, self.items):
            if isinstance(item_origin, Feature) and isinstance(item, Feature):
                assert id(item_origin.graph.nodes) == id(item.graph.nodes)

    def _subset_single_column(self, column: str) -> Feature:
        return self.feature_objects[column]

    def _subset_list_of_columns(self, columns: list[str]) -> FeatureGroup:
        return FeatureGroup([self.feature_objects[elem] for elem in columns])

    def __getitem__(self, item: str | list[str]) -> Feature | FeatureGroup:
        if isinstance(item, str):
            return self._subset_single_column(item)
        if isinstance(item, list) and all(isinstance(elem, str) for elem in item):
            return self._subset_list_of_columns(item)
        raise TypeError(f"Feature retrieval with value '{item}' is not supported!")

    def drop(self, items: list[str]) -> FeatureGroup:
        """
        Drop feature(s) from the FeatureGroup/FeatureList

        Parameters
        ----------
        items: list[str]
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


class FeatureGroup(BaseFeatureGroup):
    """
    FeatureGroup class
    """

    def __setitem__(self, key: str, value: Feature) -> None:
        # Note: since parse_obj_as() makes a copy, the changes below don't apply to the original
        # Feature object
        value = parse_obj_as(Feature, value)

        # For now, only allow assigning a feature to FeatureGroup if
        # 1. The feature is unnamed (e.g. created on-the-fly by combining different features); or
        # 2. The feature already has a name and is assigned with the same name
        if value.name is not None and value.name != key:
            raise ValueError(
                f'Feature "{value.name}" cannot be added to FeatureGroup under a different name '
                f'"{key}"'
            )

        new_node = GlobalQueryGraph().add_operation(
            node_type=NodeType.ALIAS,
            node_params={"name": key},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[value.node],
        )
        value.__dict__["node"] = new_node
        value.name = key

        self.feature_objects[key] = value
        # sanity check: make sure we don't copy global query graph
        assert id(self.feature_objects[key].graph.nodes) == id(value.graph.nodes)

    def preview(
        self,
        point_in_time_and_serving_name: dict[str, Any],
        credentials: Credentials | None = None,
    ) -> pd.DataFrame:
        """
        Preview a FeatureGroup

        Parameters
        ----------
        point_in_time_and_serving_name : dict
            Dictionary consisting the point in time and serving names based on which the feature
            preview will be computed
        credentials: Credentials | None
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


class FeatureList(BaseFeatureGroup, FeatureListModel):
    """
    FeatureList class

    items : list[Union[Feature, BaseFeatureGroup]]
        List of feature like objects to be used to create the FeatureList
    name : str
        Name of the FeatureList
    """

    @root_validator()
    @classmethod
    def _initialize_feature_list_parameters(cls, values: dict[str, Any]) -> dict[str, Any]:
        values["readiness"] = min(
            values["feature_objects"].values(),
            key=lambda feature: FeatureReadiness(feature.readiness or FeatureReadiness.min()),
        ).readiness
        values["features"] = [
            (feature.name, feature.version) for feature in values["feature_objects"].values()
        ]
        values["status"] = FeatureListStatus.DRAFT
        values["version"] = get_version()
        return values

    def get_historical_features(
        self,
        training_events: pd.DataFrame,
        credentials: Credentials | None = None,
        serving_names_mapping: dict[str, str] | None = None,
    ) -> pd.DataFrame:
        """Get historical features

        Parameters
        ----------
        training_events : pd.DataFrame
            Training events DataFrame
        credentials : Credentials | None
            Optional feature store to credential mapping
        serving_names_mapping : dict[str, str] | None
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
